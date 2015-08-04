#!/usr/bin/env python
# encoding: utf-8
"""
politwoops-worker.py

Created by Breyten Ernsting on 2010-05-30.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import time
import mimetypes
import argparse
import MySQLdb
import anyjson
import smtplib
import signal
import pytz
from email.mime.text import MIMEText
from datetime import datetime

import socket
# disable buffering
socket._fileobject.default_bufsize = 0

import httplib
httplib.HTTPConnection.debuglevel = 1

import urllib3
import elasticsearch
import anyjson
import logbook
import tweetsclient
import politwoops
replace_highpoints = politwoops.utils.replace_highpoints

_script_ = (os.path.basename(__file__)
            if __name__ == "__main__"
            else __name__)
log = logbook.Logger(_script_)

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


class DeletedTweetsWorker(object):
    def __init__(self, heart, images):
        self.heart = heart
        self.images = images
        self.get_config()

    def init_database(self):
        log.debug("Making DB connection")
        self.database = MySQLdb.connect(
            host=self.config.get('database', 'host'),
            port=int(self.config.get('database', 'port')),
            db=self.config.get('database', 'database'),
            user=self.config.get('database', 'username'),
            passwd=self.config.get('database', 'password'),
            charset="utf8",
            use_unicode=True
        )
        self.database.autocommit(True) # needed if you're using InnoDB
        self.database.cursor().execute('SET NAMES UTF8')

    def init_elasticsearch(self):
        self.es = elasticsearch.Elasticsearch(
            [self.config.get('elasticsearch', 'host')],
            # http_auth=('user', 'secret'),
            port=self.config.get('elasticsearch', 'port'),
            # use_ssl=False,
            # verify_certs=True,
            # ca_certs=certifi.where(),
        )

    def init_beanstalk(self):
        tweets_tube = self.config.get('beanstalk', 'tweets_tube')
        screenshot_tube = self.config.get('beanstalk', 'screenshot_tube')

        log.info("Initiating beanstalk connection. Watching {watch}.", watch=tweets_tube)
        if self.images:
            log.info("Queueing screenshots to {use}.", use=screenshot_tube)

        self.beanstalk = politwoops.utils.beanstalk(host=self.config.get('beanstalk', 'host'),
                                                    port=int(self.config.get('beanstalk', 'port')),
                                                    watch=tweets_tube,
                                                    use=screenshot_tube)

    # TODO: move to shared/utils file
    def load_plugin(self, plugin_module, plugin_class):
        pluginModule = __import__(plugin_module)
        components = plugin_module.split('.')
        for comp in components[1:]:
            pluginModule = getattr(pluginModule, comp)
        pluginClass = getattr(pluginModule, plugin_class)
        return pluginClass
    def get_config_default(self, section, key, default = None):
        try:
            return self.config.get(section, key)
        except ConfigParser.NoOptionError:
            return default



    def _database_keepalive(self):
        cur = self.database.cursor()
        cur.execute("""SELECT id FROM tweets LIMIT 1""")
        cur.fetchone()
        cur.close()
        log.info("Executed database connection keepalive query.")

    def get_config(self):
        log.debug("Reading config ...")
        self.config = tweetsclient.Config().get()

    def get_users(self):
        track_module = self.get_config_default('tweets-client', 'track-module', 'tweetsclient.config_track')
        track_class = self.get_config_default('tweets-client', 'track-class', 'ConfigTrackPlugin')

        pluginClass = self.load_plugin(track_module, track_class)
        self.track = pluginClass()
        users_to_track = self.track.get_items()
        ids = {}
        politicians = {}

        for user in users_to_track:
            ids[int(user["id"])] = int(user["id"]) # coping with the original politwoops architecture
            ids[int(user["id"])] = user["handle"]

        # cursor = self.database.cursor()
        # q = "SELECT `twitter_id`, `user_name`, `id` FROM `politicians`"
        # cursor.execute(q)
        # ids = {}
        # politicians = {}
        # for t in cursor.fetchall():
        #     ids[t[0]] = t[2]
        #     politicians[t[0]] = t[1]

        log.info("Found ids: {ids}", ids=ids)
        log.info("Found politicians: {politicians}", politicians=politicians)
        return ids, politicians

    def run(self):
        mimetypes.init()
        self.init_elasticsearch()
        self.init_beanstalk()
        self.users, self.politicians = self.get_users()

        while True:
            time.sleep(0.2)
            if self.heart.beat():
                pass
            #     # self._database_keepalive()
            reserve_timeout = max(self.heart.interval.total_seconds() * 0.1, 2)
            job = self.beanstalk.reserve(timeout=reserve_timeout)
            if job:
                self.handle_tweet(job.body)
                job.delete()

    def handle_tweet(self, job_body):
        tweet = anyjson.deserialize(job_body)
        if tweet.has_key('delete'):
            if tweet['delete']['status']['user_id'] in self.users.keys():
                self.handle_deletion(tweet)
        else:
            if tweet.has_key('user') and (tweet['user']['id'] in self.users.keys()):
                self.handle_new(tweet)

                if self.images and tweet.has_key('entities'):
                    # Queue the tweet for screenshots and/or image mirroring
                    log.notice("Queued tweet {0} for entity archiving.", tweet['id'])
                    self.beanstalk.put(anyjson.serialize(tweet))


    def handle_deletion(self, tweet):
        log.notice("Deleted tweet {0}", tweet['delete']['status']['id'])
        # cursor = self.database.cursor()
        # cursor.execute("""SELECT COUNT(*) FROM `tweets` WHERE `id` = %s""", (tweet['delete']['status']['id'],))
        # num_previous = cursor.fetchone()[0]
        res = self.es.search(index=self.config.get('elasticsearch', 'index'), 
                             body={"query": {"term": {"platform_id":  tweet['delete']['status']['id']  }}} )
        num_previous = res["hits"]["total"]

        daily_worker_id = 'twitter' + str(tweet['delete']['status']['id'])


        if num_previous > 0:
            self.es.update(self.config.get('elasticsearch', 'index'), 'tweet', daily_worker_id, {
                        "doc": {"deleted": True}
                })
        else:
            print(tweet)
            self.es.index(self.config.get('elasticsearch', 'index'), 'tweet', {
                        "platform": 'twitter',
                        "dw_source": 'politwoops',
                        "body":     replace_highpoints(tweet['text']),
                        "candidate_name": 'TK', # how is this going to access the instance var @screen_names above???
                        "created_at": tweet["created_at"],
                        "platform_id": tweet['delete']['status']['id'],
                        "id": 'twitter' + str(tweet['id']),
                        "_id": 'twitter' + str(tweet['id']),
                        
                        "link": "http://twitter.com/" + tweet['user']['screen_name'] + '/status/' + str(tweet['id']),
                        "deleted": True, 
                        "geo": tweet['coordinates'],
                        "platform_username": tweet['user']['screen_name'],
                        "platform_displayname": tweet['user']['name'],
                        "platform_userid": tweet['user']['id'],
                        "favorite_count": tweet['favorite_count'],
                        "retweet_count": tweet['retweet_count'],
                        "source": str(tweet['source']).replace("<a href=\"http://twitter.com\" rel=\"nofollow\">", '').replace("</a>", '')
                }, daily_worker_id)
        res = self.es.search(index=self.config.get('elasticsearch', 'index'), body={"query": {"term": {"platform_id":  tweet['delete']['status']['id']  }}} )

        # cursor.execute("""SELECT * FROM `tweets` WHERE `id` = %s""", (tweet['delete']['status']['id'],))
        ref_tweet = res['hits']['hits'][0]
        # self.send_alert(ref_tweet[1], ref_tweet[4], ref_tweet[2])

    def handle_new(self, tweet):
        log.notice("New tweet {tweet} from user {user_id}/{screen_name}",
                  tweet=tweet.get('id'),
                  user_id=tweet.get('user', {}).get('id'),
                  screen_name=tweet.get('user', {}).get('screen_name'))

        self.handle_possible_rename(tweet)

        res = self.es.search(index=self.config.get('elasticsearch', 'index'), 
                             body={"query": {"term": {"platform_id":  tweet['id']  }}} )

        num_previous = res["hits"]["total"]
        if len(res["hits"]["hits"]) > 0:
            was_deleted = (res["hits"]["hits"][0]["deleted"] == True)
        else:
            was_deleted = False
        # cursor.execute("""SELECT COUNT(*) FROM `tweets`""")
        # total_count = cursor.fetchone()[0]
        # self._debug("Total count in table: %s" % total_count)

        retweeted_id = None
        retweeted_content = None
        retweeted_user_name = None
        if tweet.has_key('retweeted_status'):
            retweeted_id = tweet['retweeted_status']['id']
            retweeted_content = replace_highpoints(tweet['retweeted_status']['text'])
            retweeted_user_name = tweet['retweeted_status']['user']['screen_name']

        daily_worker_id = 'twitter' + str(tweet['id'])

        if num_previous > 0:
            self.es.update(self.config.get('elasticsearch', 'index'), 'tweet', daily_worker_id, {
                        "platform": 'twitter',
                        "dw_source": 'politwoops',
                        "body":     replace_highpoints(tweet['text']),
                        "candidate_name": 'TK', # how is this going to access the instance var @screen_names above???
                        "created_at": tweet["created_at"],
                        "platform_id": tweet['id'],
                        "id": 'twitter' + str(tweet['id']),
                        "_id": 'twitter' + str(tweet['id']),
                        
                        "link": "http://twitter.com/" + tweet['user']['screen_name'] + '/status/' + str(tweet['id']),
                        "deleted": False, 
                        "geo": tweet['coordinates'],
                        "platform_username": tweet['user']['screen_name'],
                        "platform_displayname": tweet['user']['name'],
                        "platform_userid": tweet['user']['id'],
                        "favorite_count": tweet['favorite_count'],
                        "retweet_count": tweet['retweet_count'],
                        "source": str(tweet['source']).replace("<a href=\"http://twitter.com\" rel=\"nofollow\">", '').replace("</a>", '')
                })

            # index – The name of the index
            # doc_type – The type of the document
            # id – Document ID
            # body – The request definition using either script or partial doc
            log.info("Updated tweet {0}", tweet.get('id'))
        else:
            self.es.index(self.config.get('elasticsearch', 'index'), 'tweet', {
                        "platform": 'twitter',
                        "dw_source": 'politwoops',
                        "body":     replace_highpoints(tweet['text']),
                        "candidate_name": 'TK', # how is this going to access the instance var @screen_names above???
                        "created_at": tweet["created_at"],
                        "platform_id": tweet['id'],
                        "id": 'twitter' + str(tweet['id']),
                        "_id": 'twitter' + str(tweet['id']),
                        
                        "link": "http://twitter.com/" + tweet['user']['screen_name'] + '/status/' + str(tweet['id']),
                        "deleted": was_deleted, 
                        "geo": tweet['coordinates'],
                        "platform_username": tweet['user']['screen_name'],
                        "platform_displayname": tweet['user']['name'],
                        "platform_userid": tweet['user']['id'],
                        "favorite_count": tweet['favorite_count'],
                        "retweet_count": tweet['retweet_count'],
                        "source": str(tweet['source']).replace("<a href=\"http://twitter.com\" rel=\"nofollow\">", '').replace("</a>", '')
                }, daily_worker_id)
            log.info("Inserted new tweet {0}", tweet.get('id'))


        if was_deleted:
            log.warn("Tweet deleted {0} before it came!", tweet.get('id'))
            self.copy_tweet_to_deleted_table(tweet['id'])

    def copy_tweet_to_deleted_table(self, tweet_id):
        # cursor = self.database.cursor()
        # cursor.execute("""REPLACE INTO `deleted_tweets` SELECT * FROM `tweets` WHERE `id` = %s AND `content` IS NOT NULL""" % (tweet_id))
        return

    def handle_possible_rename(self, tweet):
        return
        # tweet_user_name = tweet['user']['screen_name']
        # tweet_user_id = tweet['user']['id']
        # current_user_name = self.politicians[tweet_user_id]
        # if current_user_name != tweet_user_name:
        #     self.politicians[tweet_user_id] = tweet_user_name
        #     cursor= self.database.cursor()
        #     cursor.execute("""UPDATE `politicians` SET `user_name` = %s WHERE `id` = %s""", (tweet_user_name, self.users[tweet_user_id]))


    def send_alert(self, username, created, text):
        return
        if username and self.config.has_section('moderation-alerts'):
            host = self.config.get('moderation-alerts', 'mail_host')
            port = self.config.get('moderation-alerts', 'mail_port')
            user = self.config.get('moderation-alerts', 'mail_username')
            password = self.config.get('moderation-alerts', 'mail_password')
            recipient = self.config.get('moderation-alerts', 'twoops_recipient')
            sender = self.config.get('moderation-alerts', 'sender')

            if not text:
                #in case text is None from a deleted but not originally captured deleted tweet
                text = ''
            text += "\n\nModerate this deletion here: http://politwoops.sunlightfoundation.com/admin/review\n\nEmail the moderation group if you have questions or would like a second opinion at politwoops-moderation@sunlightfoundation.com"

            nowtime = datetime.now()
            diff = nowtime - created
            diffstr = ''
            if diff.days != 0:
                diffstr += '%s days' % diff.days
            else:
                if diff.seconds > 86400:
                    diffstr += "%s days" % (diff.seconds / 86400 )
                elif diff.seconds > 3600:
                    diffstr += "%s hours" % (diff.seconds / 3600)
                elif diff.seconds > 60:
                    diffstr += "%s minutes" % (diff.seconds / 60)
                else:
                    diffstr += "%s seconds" % diff.seconds

            nowtime = pytz.timezone('UTC').localize(nowtime)
            nowtime = nowtime.astimezone(pytz.timezone('US/Eastern'))

            smtp = smtplib.SMTP(host, port)
            smtp.login(user, password)
            msg = MIMEText(text.encode('UTF-8'), 'plain', 'UTF-8')
            msg['Subject'] = 'Politwoop! @%s -- deleted on %s after %s' % (username, nowtime.strftime('%m-%d-%Y %I:%M %p'), diffstr)
            msg['From'] = sender
            msg['To'] = recipient
            smtp.sendmail(sender, recipient, msg.as_string())


def main(args):
    signal.signal(signal.SIGHUP, politwoops.utils.restart_process)

    log_handler = politwoops.utils.configure_log_handler(_script_, args.loglevel, args.output)
    with logbook.NullHandler():
        with log_handler.applicationbound():
            try:
                log.info("Starting Politwoops worker...")
                log.notice("Log level {0}".format(log_handler.level_name))
                if args.images:
                    log.notice("Screenshot support enabled.")

                with politwoops.utils.Heart() as heart:
                    politwoops.utils.start_watchdog_thread(heart)
                    app = DeletedTweetsWorker(heart, args.images)
                    if args.restart:
                        return politwoops.utils.run_with_restart(app.run)
                    else:
                        try:
                            return app.run()
                        except Exception as e:
                            logbook.error("Unhandled exception of type {exctype}: {exception}",
                                          exctype=type(e),
                                          exception=str(e))
                            if not args.restart:
                                raise

            except KeyboardInterrupt:
                log.notice("Killed by CTRL-C")


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description=__doc__)
    args_parser.add_argument('--loglevel', metavar='LEVEL', type=str,
                             help='Logging level (default: notice)',
                             default='notice',
                             choices=('debug', 'info', 'notice', 'warning',
                                      'error', 'critical'))
    args_parser.add_argument('--output', metavar='DEST', type=str,
                             default='-',
                             help='Destination for log output (-, syslog, or filename)')
    args_parser.add_argument('--images', default=False, action='store_true',
                             help='Whether to screenshot links or mirror images linked in tweets.')
    args_parser.add_argument('--restart', default=False, action='store_true',
                             help='Restart when an error cannot be handled.')

    args = args_parser.parse_args()
    sys.exit(main(args))
