from os import listdir
import alignapy
from alignapy import *
from requests.exceptions import MissingSchema
import datetime
from mongoengine import *
import multiprocessing



SUBS_DIR='/home/mikel/doctorado/subgraphs/subs/'
#ALIGN_LIST=['NameAndPropertyAlignment', 'StringDistAlignment', 'NameEqAlignment', 'EditDistNameAlignment', 'SMOANameAlignment', 'SubsDistNameAlignment', 'JWNLAlignment']
ALIGN_LIST=['JWNLAlignment']
POOL_SIZE=10

class MongoAlign(Document):
    source_onto = StringField(required=True)
    target_onto = StringField(required=True)
    source_obj = StringField(required=True)
    target_obj = StringField(required=True)
    align_class = StringField(required=True)
    score = FloatField(required=True)
    
class BlackList(Document):
    ontology = StringField(required=True)
    last_access = DateTimeField()


class Graph():
    def __init__(self):
        self.vertex_list = []
        self.edge_list = []
        
    def add_vertex(self, vertex):
        self.vertex_list.append(vertex)
        
    def add_edge(self, edge):
        self.edge_list.append(edge)

class Vertex():
    def __init__(self, vertex_id, name):
        self.vertex_id = vertex_id
        self.vertex_id = name

class Edge():
    def __init__(self, source, target, name):
        self.source = source
        self.target = target
        self.name = name

def blacklist_add(uri):
    dt = datetime.datetime.now()
    bl = BlackList(ontology=uri, last_access=dt)
    bl.save()

def get_base(uri):
    if '#' in uri:
        return uri.split('#')[0].replace('<', '') + '#'
    else:
        chunks = uri.split('/')
        base = ''
        for i in xrange(len(chunks) - 1):
            base += chunks[i] + '/'
        return base.replace('<', '')

def process((ap, o1, o2, align_class)):
    try:
        if len(BlackList.objects(ontology=o1)) <= 0 and len(BlackList.objects(ontology=o2)) <= 0 and o1 != o2:
            alignment = MongoAlign.objects(source_onto=o1, target_onto=o2, align_class=align_class)
            if len(alignment) <= 0:
                ap.init(o1, o2)
                ap.align()
                print 'cells', len(ap.cell_list)
                for cell in ap.cell_list:
                    #print str(cell.prop1[0]), str(cell.prop2[0]), cell.measure
                    try:
                        mongo = MongoAlign(source_onto=o1, target_onto=o2, source_obj=str(cell.prop1[0]), target_obj=str(cell.prop2[0]), align_class=align_class, score=cell.measure)
                        mongo.save()
                    except Exception as e:
                        print e
    except UriNotFound as e:
        blacklist_add(e.uri)
    except MissingSchema as e:
        #print 'Incorrect schema'
        print e.args
    except UnsupportedContent as e:
        print e
        blacklist_add(e.uri)
    except IncorrectMimeType as e:
        print e
        blacklist_add(e.uri)
    except Exception as e:
        pass


connect('alignment')

graph_dict = {}
ontology_list = []
for sub in listdir(SUBS_DIR):
    f = open(SUBS_DIR + '/' + sub)
    graph = Graph()
    for line in f:
        if line.find('v') == 0:
            chunks = line.split(' ')
            vertex_id = chunks[1]
            vertex_name = chunks[2].replace('\n', '')
            base = get_base(vertex_name)
            if base not in ontology_list:
                ontology_list.append(base)
            vertex = Vertex(vertex_id, vertex_name)
            graph.add_vertex(vertex)
        elif line.find('d') == 0:
            chunks = line.split(' ')
            source = chunks[1]
            target = chunks[2]
            name = chunks[3].replace('"', '')
            base = get_base(name)
            if base not in ontology_list:
                ontology_list.append(base)
            edge = Edge(source, target, name)
            graph.add_edge(edge)
    graph_dict[sub] = graph
    f.close()

ap_list = []

for o1 in ontology_list:
    for o2 in ontology_list:
        #if len(BlackList.objects(ontology=o1)) <= 0 and len(BlackList.objects(ontology=o2)) <= 0 and o1 != o2:
        for align_class in ALIGN_LIST:
            #alignment = MongoAlign.objects(source_onto=o1, target_onto=o2, align_class=align_class)
        #if len(alignment) <= 0:
            #print 'Creating alignment between %s and %s using %s' % (o1, o2, align_class)
            ap = None
            ap = getattr(alignapy, align_class)()
            ap_list.append((ap, o1, o2, align_class))
                
#result = map(process, ap_list)
pool = multiprocessing.Pool(POOL_SIZE)
result = pool.map(process, ap_list)         


'''for key1 in graph_dict:
    g1 = graph_dict[key1]
    for key2 in graph_dict:
        if key1 != key2:
            g2 = graph_dict[key2]
            for 
'''
