#!/usr/bin/env python



import django.utils.encoding
import json
import memcache
import Queue
import socket
import sys
import threading
import time
import urllib
import urllib2

from kb.nl.api import oai

GET_IR = "http://kbresearch.nl/get_ir/?identifier="

GET_NIR = "http://kbresearch.nl/get_nir/?identifier="
RESOLVER_PREFIX = "http://resolver.kb.nl/resolve?urn="
WORKERS = 20

mc = memcache.Client(['127.0.0.1:11211'], debug=False)

done = False
socket.setdefaulttimeout(920)

# Do not index entities with these version.
unwanted_entities = ['kranten-entities-3', 'kranten-entities-2',
                     'kranten-entities-1', 'kranten-entities-4']

def wait_for_lock():
    global mc

    while mc.get('index_please_lock'):
        print('locked, waiting')
        time.sleep(0.01)
    return

class ir_thread(threading.Thread):
    '''Fetch infromation resources
        class responsible for forming an update object,
        done mainly by talking to get_ir and get_nir.
    '''
    errors = 0
    def __init__(self, ir_que, solr_que):
        threading.Thread.__init__(self)
        self.ir_que = ir_que
        self.solr_que = solr_que
        self.done = False
        self.daemon = True

    def run(self):
        while not self.done:
            while self.ir_que.empty():
                time.sleep(0.5)
            while not self.ir_que.empty():
                identifier = self.ir_que.get()
                data = self.get_ir_data(identifier)

                #dacresult
                #print(data, identifier)
                #with open('/tmp/solr', 'w') as fh:
                #    fh.write(str(data))
                if data:
                    self.solr_que.put(data)

    def get_ir_data(self, identifier):
        url = GET_IR + RESOLVER_PREFIX + identifier + ':ocr'
        print(url)
        wanted = 'http://resolver.kb.nl/resolve?urn=ddd:010543002:mpeg21:a0026:ocr'
        if url.find(wanted) > -1:
            print('************************')

        try:
            ir = json.loads(urllib.urlopen(url).read())
        except:
            self.errors += 1
            print("Error: get_ir_data, num errors: %i" % (self.errors))
            self.ir_que.put(identifier)
            return False

        load_string = u'[{"uniqueKey":"' + identifier + u'",'
        error = ''
        header = ir.get('header')
        if header:
            print(header.get('status'))
            if not header.get('status') == 'OK':
                error = header.get('message')
        load_string += 'dacresult:{"set":"%s"},' % error


        if ir.get('links'):
            places, latlong = self.parse_ir_data_places(ir.get('links'))
            identifiers = self.parse_ir_data_identifiers(ir.get('links'), latlong)

            if places or identifiers:
                load_string += places
                if identifiers and not load_string.endswith(','):
                    load_string += ","
                load_string += identifiers

        if load_string.endswith(','):
            load_string = load_string[:-1]

        load_string += u'}]'
        print(identifier, load_string)

        return load_string


    def parse_ir_data_places(self, links):
        places = []
        load_string = ""
        latlong = []

        for item in links:
            if item.get("linkType") == "street":
                place = item.get("place")
                street = item.get("street")
                latlong = item.get("latlong")
                places.append({"place" : place, "street" : street, "latlong" : latlong})

        if places:
            #latlong = '","'.join([i.get('latlong') for i in places])
            latlong = [i.get('latlong') for i in places]
            place = '","'.join([i.get('place') for i in places])
            street = '","'.join([i.get('street') for i in places])

            #load_string += u'"latlong":{"set" :["'+ latlong + '"]},'
            load_string += u'"street":{"set" :["'+ street +'"]},'
            load_string += u'"place":{"set" :["'+ place + '"]},'

        if load_string.endswith(','):
            load_string = load_string[:-1]

        return load_string, latlong

    def parse_ir_data_identifiers(self, links, latlong):
        named_entities = []
        load_string = ""
        nir = {}

        references = []
        fb_id = []
        geo_id = []
        ppn_id = []
        viaf_id = []
        wd_id = []
        pdc_id = []

        for item in links:
            if item.get("linkType") == "NIR" and not item.get("reference") in unwanted_entities:
                if nir.get("error") and nir.get("error").startswith(GET_NIR):
                    url = nir.get("error")
                else:
                    url = GET_NIR + item.get("id")
                try:
                    #print(django.utils.encoding.iri_to_uri(url))
                    nir = json.loads(urllib.urlopen(django.utils.encoding.iri_to_uri(url)).read())
                except:
                    nir = {"error" : url}

                if nir.get("error"):
                    continue

                fb = geo = ppn = viaf = wd = pdc = ""

                if nir.get('enrich'):
                    fb = [i for i in nir.get('enrich') if i.get('linkType') == 'FB']
                    geo = [i for i in nir.get('enrich') if i.get('linkType') == 'GEO']
                    ppn = [i for i in nir.get('enrich') if i.get('linkType') == 'PPN']
                    viaf = [i for i in nir.get('enrich') if i.get('linkType') == 'VIAF']
                    wd = [i for i in nir.get('enrich') if i.get('linkType') == 'WD']
                    pdc = [i for i in nir.get('enrich') if i.get('linkType') == 'PDC']

                if i.get('linkType') == 'location':
                    if not i.get('latlong') in latlong:
                        latlong.append(i.get('latlong'))

                if fb:
                    fb = ".".join(fb[0].get('sameAs').split('/')[-2:])
                if geo:
                    geo = ".".join(geo[0].get('sameAs').split('/')[-1:])
                if wd:
                    wd = ".".join(wd[0].get('sameAs').split('/')[-1:])
                if viaf:
                    viaf = ".".join(viaf[0].get('sameAs').split('/')[-1:])
                if ppn:
                    ppn = ".".join(ppn[0].get('sameAs').split('/')[-1:])
                if pdc:
                    pdc = ".".join(pdc[0].get('sameAs').split('/')[-1:])

                if not item.get('objectName') in named_entities:
                    named_entities.append(item.get('objectName'))
                if not item.get('reference') in references:
                    references.append(item.get('reference'))

                if fb and not fb in fb_id:
                    fb_id.append(fb)
                if geo and not geo in geo_id:
                    geo_id.append(geo)
                if viaf and not viaf in viaf_id:
                    viaf_id.append(viaf)
                if wd and not wd in wd_id:
                    wd_id.append(wd)
                if ppn and not ppn in ppn_id:
                    ppn_id.append(ppn)
                if pdc and not pdc in pdc_id:
                    pdc_id.append(pdc)

        if named_entities:
            load_string += u'"reference":{"set":["' + u'","'.join(references) + u'"]},'
            load_string += u'"name":{"set":["' + u'","'.join(named_entities) + u'"]},'

            if latlong:
                load_string += u'"latlong":{"set" :["'+ u'","'.join(latlong) + '"]},'

            if wd_id:
                load_string += u'"wd_id":{"set":["' + u'","'.join(wd_id) + u'"]},'

            if geo_id:
                load_string += u'"geo_id":{"set":["' + u'","'.join(geo_id) + u'"]},'

            if viaf_id:
                load_string += u'"viaf_id":{"set":["' + u'","'.join(viaf_id) + u'"]},'

            if ppn_id:
                load_string += u'"ppn_id":{"set":["' + u'","'.join(ppn_id) + u'"]},'

            if fb_id:
                load_string += u'"fb_id":{"set":["' + u'","'.join(fb_id) + u'"]},'

            if pdc_id:
                load_string += u'"pdc_id":{"set":["' + u'","'.join(pdc_id) + u'"]}'


        if load_string.endswith(','):
            load_string = load_string[:-1]
        return load_string

class solr_thread(threading.Thread):
    '''This thread is talking to solr, to insert new data'''
    error = 0
    written = 0
    def __init__(self, solr_que):
        threading.Thread.__init__(self)
        self.solr_que = solr_que
        self.daemon  = True
        self.done = False

    def run(self):
        while not self.done:
            while self.solr_que.empty():
                time.sleep(0.5)
            load_string = self.solr_que.get()
            done = False
            retry = 0
            while not done:
                try:
                    self.written += 1
                    req = urllib2.Request('http://localhost:8983/solr/DDD_artikel_research/update') #?commit=true')
                    req.add_header('Content-Type', 'application/json; charset=utf-8')
                    response = urllib2.urlopen(req, load_string.encode('utf-8'))
                    if not response.code == 200:
                        print (response.code)
                    else:
                        #print("Done: ", load_string)
                        #print(self.written, response.code, load_string.encode('utf-8'))
                        #print(str(response.code) +'\n')
                        print('.')
                        done = True
                except:
                    retry += 1
                    print('*****', load_string)
                    time.sleep(1)
                    if retry > 10:
                        done = True
                        self.error += 1
                        print ("SOLR failed (after 10 retry's!), num errors: \
                                %i" % self.error)
                        print(load_string)
                        retry = 0

ir_que = Queue.Queue()
solr_que = Queue.Queue()

ir_workers = []
solr_workers = []


for i in range(WORKERS):
    worker = ir_thread(ir_que, solr_que)
    worker.start()
    ir_workers.append(worker)

for i in range(WORKERS):
    worker = solr_thread(solr_que)
    worker.start()
    solr_workers.append(worker)

while True:
    # Start lock
    wait_for_lock()
    mc.set('index_please_lock', True)
    # Fetch recods
    todo = mc.get('index_please')
    mc.set('index_please', [])
    mc.set('index_please_lock', False)

    # Iterate over object to update.
    if todo is None:
        pass
    else:
        for line in todo:
            if line:
                #print("Indexing " + line)
                ir_que.put(line.replace(":ocr",""))
                while ir_que.qsize() > 9000:
                    print("Warning: IR_que > 9000000")
                    print("ir_que:", ir_que.qsize())
                    time.sleep(2)
                #line = fh.readline().strip()
    time.sleep(0.5)


for i in ir_workers:
    i.join()

for i in solr_workers:
    i.join()
