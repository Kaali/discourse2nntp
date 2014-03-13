import datetime
import email.Utils
import iso8601
import json
import md5
import socket
import sys
import time
import urllib

from cStringIO import StringIO
from email.Message import Message
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from twisted.internet import reactor, defer
from twisted.news import database, news, nntp
from twisted.python import log
from twisted.web import client
from zope.interface import implements

DEBUG = False
API_HOST = ''
API_KEY = ''
API_USERNAME = ''
API_URL = ''

# TODO: Do not hardcode categories
CATEGORIES = ['category/yleista.json',
              'category/ohjelmointi.json',
              'category/tiedotus.json',
              'category/vapaa-aika.json',
              'category/meta.json',
              'category/huuhaa.json',
              'category/blog.json',
              'category/testi.json',
              'category/pihvi.json',
          ]

def api_get(path, params=None):
    if params is None:
        params = {}
    else:
        params = params.copy()
    defaultParams = {'api_key': API_KEY, 'api_username': API_USERNAME}
    params.update(defaultParams)
    
    data = urllib.urlencode(params)
    url = "%s/%s?%s" % (API_URL, path, data)
    debug('API_GET %s', (url,))
    return client.getPage(url)

def debug(msg, params=()):
    if DEBUG:
        print msg % params

# TODO: Rename topics as posts where applicable
class Category(object):
    refreshRate = 60 * 5

    def __init__(self, path):
        self.title = ""
        self.path = path
        self.topics = []
        self.topicByIdMap = {}
        self.postNumberSeen = set()
        self.lastRefreshTime = 0
        self.refreshing = None

    def topicByIdx(self, idx):
        """Return topic using NNTP article id"""
        return self.topics[idx - 1]

    def topicByMessageId(self, id):
        """Return topic by mail Message-Id"""
        return self.topicByIdMap[id]

    def postsByTopicId(self, topicId):
        """Return posts for topicId in post number sorted order"""
        return sorted([x for x in self.topics if x['TopicId'] == str(topicId)],
                      key=lambda x: x['PostNumber'])
    
    def hasTopicId(self, id):
        return id in self.topicByIdMap
    
    def topicCount(self):
        return len(self.topics)

    def parsePostsFromTopicJson(self, topicJson, postJson):
        guid = md5.md5(str(postJson['topic_id']) +
                       str(postJson['id'])).hexdigest()
        messageId = '<%s@%s>' % (guid, API_HOST)
        if messageId in self.topicByIdMap:
            # TODO: Detect update/edit
            debug('detected duplicate topic')
            return None

        topic = MIMEMultipart('alternative')
        # TODO: Sometimes From is shown as empty in Gnus (invalid data error)
        topic['From'] = Header(postJson['name'], 'utf8')
        topic['Newsgroups'] = self.path
        topic['Message-Id'] = messageId
        topic['Subject'] = Header(topicJson['title'], 'utf8')
        # TODO: Fix timezone error
        topic['Date'] = email.Utils.formatdate(
            time.mktime(
                iso8601.parse_date(postJson['created_at']).utctimetuple()))

        # Discourse stuff
        topic['PostNumber'] = str(postJson['post_number'])
        topic['ReplyToPostNumber'] = str(postJson['reply_to_post_number'])
        topic['TopicId'] = str(postJson['topic_id'])
        topic['DiscourseLink'] = (API_URL +
                                  '/t/%s/%i/%i' % (topicJson['slug'],
                                                   topicJson['id'],
                                                   postJson['post_number']))
        # TODO: Fix relative urls in body
        # TODO: Youtube inline videos not in cooked?
        bodyText = postJson['cooked'].encode('utf8', errors='xmlcharrefreplace')
        body = MIMEText(bodyText, 'html')
        topic.attach(body)
        return topic

    def refreshIfNeeded(self):
        timeSinceRefresh = time.time() - self.lastRefreshTime
        if timeSinceRefresh > self.refreshRate:
            if not self.refreshing:
                debug("refreshing")
                self.refreshing = self._refresh()
            d = defer.Deferred()
            self.refreshing.chainDeferred(d)
            return d
        else:
            return defer.succeed(None)

    @defer.inlineCallbacks
    def _refresh(self):
        debug('Start refresh')
        # Get the category
        try:
            categoryJson = yield api_get(self.path)
            categoryJson = json.loads(categoryJson)
            debug('Loaded topic data from %s', (self.path))
        except Exception, err:
            print 'Failed to get topics for "%s"' % self.path
            raise err

        # Get topic jsons for category
        # TODO: How to detect only new topics?
        #       - By message count? But that wouldn't support post update
        topicIds = [x['id'] for x in categoryJson[u'topic_list'][u'topics']]
        for topicId in topicIds:
            topicJson = yield api_get('t/%s.json' % topicId)
            posts = yield self._retrieveAndParseTopic(json.loads(topicJson))
        
        # Remove the refresh lock
        self.refreshing = None
        self.lastRefreshTime = time.time()

    @defer.inlineCallbacks
    def _retrieveAndParseTopic(self, topicJson):
        debug("Retrieving and parsing posts for topic")
        postIds = topicJson['post_stream']['stream']
        # TODO: Fetch multiple posts with one bulk get
        # TODO: Sort by post number?
        for id in postIds:
            if id in self.postNumberSeen:
                # TODO: Fix update
                debug('Skipped post fetch because number seen')
                continue
            postJson = yield api_get('t/%s/posts.json' % topicJson['id'], {"post_ids[]": id})
            postJson = json.loads(postJson)
            post = postJson['post_stream']['posts'][0]
            parsedPost = yield self._parsePost(topicJson, post)
            if parsedPost:
                self.topics.append(parsedPost)
                self.topicByIdMap[parsedPost['Message-Id']] = parsedPost
                self.postNumberSeen.add(id)
            else:
                print 'Did not parse post?'

        # TODO: Extract method
        # 1. Get posts for topic_id
        # 2. Get first post number from topic posts
        # 3. Map reply to post numbers to post numbers in
        #    references with first post always assigned
        topicId = topicJson['id']
        postsForTopic = self.postsByTopicId(topicId)
        postsByPostNumber = dict((x['PostNumber'], x) for x in postsForTopic)
        firstPost = postsByPostNumber['1']
        for post in postsForTopic:
            if 'References' in post: continue
            refs = [firstPost['Message-Id']]
            replyTo = post.get('ReplyToPostNumber', None)
            # TODO: Fix recursivity
            if replyTo is not None and replyTo != 'None':
                byNum = postsByPostNumber.get(replyTo, None)
                if byNum:
                    refs.append(byNum['Message-Id'])
            post['References'] = ', '.join(refs)
        defer.returnValue(True)

    def _parsePost(self, topicJson, postJson):
        post = self.parsePostsFromTopicJson(topicJson, postJson)
        return defer.succeed(post)
    
    def groupInfo(self):
        postingAllowed = 'n'
        return (self.path, len(self.topics), 0, postingAllowed)

    def groupRequestInfo(self):
        return (self.path, len(self.topics), len(self.topics), 1, {})


class CategoryStorage(object):
    'keeps articles in memory, loses them when the process exits'
    implements(database.INewsStorage)

    def __init__(self, categories):
        self.categories = {}
        for category in categories:
            self.categories[category] = Category(category)

    def refreshAll(self):
        return defer.DeferredList(
            category.refreshIfNeeded() for category in self.categories.values())
    
    def _refreshAllAndCall(self, func, *args, **kwargs):
        return self.refreshAll().addCallback(
            lambda _: func(*args, **kwargs))

    def _refreshCategoryAndCall(self, groupName, func, *args, **kwargs):
        if groupName in self.categories:
            category = self.categories[groupName]
            return category.refreshIfNeeded().addCallback(
                lambda _: func(*args, **kwargs))
        else:
            return defer.fail(KeyError(groupName))
    
    def newgroupsRequest(self):
        # TODO: Implement real version, now always returns everything
        # TODO: Parameters
        return defer.succeed(self.categories.keys())

    def listRequest(self):
        return self._refreshAllAndCall(self._listRequest)

    def _listRequest(self):
        return [x.groupInfo() for x in self.categories.values()]
    
    def listGroupRequest(self, groupname):
        return self._refreshCategoryAndCall(groupName, self._listGroupRequest,
                                            groupName)

    def _listGroupRequest(self, groupName):
        category = self.categories[groupName]
        return range(1, category.topicCount() + 1)

    def subscriptionRequest(self):
        return defer.succeed(self.categories.keys())

    def overviewRequest(self):
        return defer.succeed(database.OVERVIEW_FMT)

    def groupRequest(self, groupName):
        return self._refreshCategoryAndCall(groupName,
                                            self._groupRequest,
                                            groupName)

    def _groupRequest(self, groupName):
        return defer.succeed(self.categories[groupName].groupRequestInfo())
    
    def xoverRequest(self, groupName, low, high):
        return self._refreshCategoryAndCall(groupName,
                                            self._processXOver,
                                            groupName, low, high,
                                            database.OVERVIEW_FMT)

    def _processXOver(self, groupName, low, high, headerNames):
        category = self.categories[groupName]
        if low is None: low = 1
        if high is None: high = category.topicCount() - 1

        results = []
        for i in range(low, high + 1):
            topic = category.topicByIdx(i)
            topicData = topic.as_string(unixfrom=False)
            headerValues = [i]
            for header in headerNames:
                if header == 'Byte-Count:':
                    headerValues.append(len(topicData))
                elif header == 'Line-Count:':
                    headerValues.append(len(topicData.split('\n')))
                else:
                    headerValues.append(topic.get(header, ''))
            results.append(headerValues)
        return defer.succeed(results)
    
    def xhdrRequest(self, groupName, low, high, header):
        return self._refreshCategoryAndCall(groupName,
                                            self._processXOver,
                                            groupName, low, high,
                                            [header])

    def articleExistsRequest(self, groupName, id):
        category = self.categories[groupName]
        return defer.succeed(category.hasTopicId(id))

    def articleRequest(self, groupName, index, id=None):
        category = self.categories[groupName]
        if id:
            topic = category.topicByMessageId(id)
            index = category.topics.index(topic) + 1
        else:
            topic = category.topicByIdx(index)
            for topicId, t, in category.topicByIdMap.items():
                if t is topic:
                    id = topicId
                    break
        topicData = topic.as_string(unixfrom=False)
        return defer.succeed((index, id, (StringIO(topicData))))

    def headRequest(self, groupName, index=None):
        category = self.categories[groupName]
        topic = category.topicByIdx(index)
        headers = '\n'.join('%s: %s' % (x[0], x[1]) for x in topic.items())
        return defer.succeed((index, topic['Message-Id'], headers))

    def bodyRequest(self, groupName, index):
        category = self.categories[groupName]
        topic = categgory.articleByIdx(index)
        return defer.succeed(topic.body)

    def postRequest(self, message):
        return defer.fail(Exception("read-only for now"))


class DebuggingNNTPProtocol(nntp.NNTPServer):
    debug = True
    COMMANDS = nntp.NNTPServer.COMMANDS
    COMMANDS.append('NEWGROUPS')

    def do_NEWGROUPS(self, date, time, timezone='GMT'):
        defer = self.factory.backend.newgroupsRequest()
        defer.addCallbacks(self._gotNewgroups, self._errNewgroups)

    def _gotNewgroups(self, groups):
        self.sendLine('231 list of new newsgroups follows')
        self.sendLine('\t'.join(groups))
        self.sendLine('.')

    def _errNewgroups(self, failure):
        print 'NEWGROUPS failed: ', failure
        self.sendLine('500 Failed to get newgroups')
    
    def lineReceived(self, line):
        if self.debug:
            print 'CLIENT:', line
        nntp.NNTPServer.lineReceived(self, line)

    def sendLine(self, line):
        nntp.NNTPServer.sendLine(self, line)
        if self.debug:
            print 'SERVER:', line


class DebuggingNNTPFactory(news.NNTPFactory):
    protocol = DebuggingNNTPProtocol

if __name__ == '__main__':
    if len(sys.argv) == 4:
        API_HOST = sys.argv[1]
        API_KEY = sys.argv[2]
        API_USERNAME = sys.argv[3]
        API_URL = 'https://' + API_HOST
    else:
        print 'Usage: %s api_host api_key api_username' % sys.argv[0]
        sys.exit(0)
    factory = DebuggingNNTPFactory(CategoryStorage(CATEGORIES))
    reactor.listenTCP(8119, factory)
    reactor.run()
