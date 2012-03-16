# -*- coding: utf-8 -*-
# Heavily based on https://gist.github.com/996120
import os
import sys
import csv

import requests
from urlparse import urlsplit, urljoin
import lxml.html

from item import Item
from request import Request
from response import Response

import gevent
from gevent import monkey, queue, Greenlet, pool, event
monkey.patch_all(thread=False)

import traceback
import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('crawly')


class Crawly(object):
    # Based on gcrawler

    def __init__(self, start_urls = [], timeout = 2, worker_count = 10, pipeline_size = 100, csv_file = None):
        self.timeout         = timeout
        self.count           = worker_count
        self.in_queue        = queue.Queue() # Queue of URL's to be scheduled
        self.out_queue       = queue.Queue(pipeline_size) # Queue to the pipeline thread
        self.worker_pool     = pool.Pool(worker_count)
        self.seen_requests   = set()
        self.allowed_domains = set()
        self.worker_finished = event.Event()

        for url in start_urls:
            self.allowed_domains.add( urlsplit(url).hostname )
            self.add_request(url)

        log.debug("Allowed domains: %s" %self.allowed_domains)

        if csv_file is not None:
            self.csv_file = csv.DictWriter(open(csv_file, 'wb'))
        else:
            self.csv_file = None


    def add_request(self, req):
        if isinstance(req, basestring):
            req = Request(req)

        if req in self.seen_requests:
            return

        self.seen_requests.add(req)
        log.debug("Adding request: %s" % req)
        self.in_queue.put(req)


    def start(self):
        # Start scheduler
        self.scheduler_greenlet = gevent.spawn(self.scheduler)

        # Start pipeline
        self.pipeline_greenlet = gevent.spawn(self.pipeline)

        # Wait until scheduler is complete
        self.scheduler_greenlet.join()


    def scheduler(self):
        log = logging.getLogger('crawly.scheduler')

        while True:
            # Remove dead greenlets
            for thread in list(self.worker_pool):
                if thread.dead:
                    self.worker_pool.discard(thread)

            # Assign requests
            try:
                req = self.in_queue.get_nowait()
                log.debug("Fetching request from in_queue")
            # If we have no more requests in queue
            except queue.Empty:
                log.debug("No requests remaining!")

                # Check and wait for existing threads to complete
                if self.worker_pool.free_count() != self.worker_pool.size:
                    log.debug("%d workers remaining, waiting.." %( self.worker_pool.size - self.worker_pool.free_count() ))
                    self.worker_finished.wait()
                    self.worker_finished.clear()
                    continue # Keep looping until
                else:
                    log.info("No workers left, shutting down!")
                    return self.shutdown()

            # Check to make sure we did not already visit this URL
            # if req in self.seen_requests:
            #     if req.
            #     log.debug("Request already handled: %s" % (req))
            #     return
            # else:
            #     self.seen_requests.add(req)

            # Check to make sure the request URL falls under our allowed domains
            if any(urlsplit(req.url).hostname == domain for domain in self.allowed_domains) is False:
                log.debug("Request hostname (%s) does not fall in the allowed domains: %s" %(urlsplit(req.url).hostname, self.allowed_domains))
                return

            # Spawn worker to handle the request
            self.worker_pool.spawn(self.worker, req)


    def shutdown(self):
        self.worker_pool.join() # Should not block
        self.out_queue.put(StopIteration) # Notify pipeline thread that we are at the end
        self.pipeline_greenlet.join() # Wait until pipeline finishes processing the last data, StopIteration
        return True


    def worker(self, req):
        log = logging.getLogger('crawly.worker')

        try:
            resp = requests.get(req.url, timeout=self.timeout)

            # Update the request
            req.response = Response(resp)

        except Exception, e:
            log.error("Error fetching: %s\n\t%s" % (req, e))
            raise gevent.GreenletExit("error")
        finally:
            self.worker_finished.set()

        # Put response on pipeline queue
        self.out_queue.put(req)

        log.debug("Fetched: %s" % req)

        raise gevent.GreenletExit("success")


    def pipeline(self):
        log = logging.getLogger('crawly.pipeline')

        # Post processing
        for req in self.out_queue: # keep going until we hit StopIteration
            try:
                # Pass the request & response to helper function to be processed
                self.process(req)
            except:
                log.error("Error:\n%s" % traceback.format_exc())

        log.debug("Pipeline complete!")


    def process(self, req):
        resp = req.response
        root = lxml.html.fromstring(resp.data)
        item = Item()
        item['title'] = root.xpath("//title/text()")[0].strip()

        log.debug('Item: %s' %item)
        self.write_csv(item)


    def write_csv(self, item):
        print "OK"

urls = ['http://appworld.blackberry.com/webstore/content/42040/?lang=en', 'http://appworld.blackberry.com/webstore/content/42040/?lang=en']
Crawly(urls).start()