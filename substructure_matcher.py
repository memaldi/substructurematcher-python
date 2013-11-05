from os import listdir
import alignapy
from alignapy import *
from requests.exceptions import MissingSchema
import datetime
from mongoengine import *
import multiprocessing
import uuid
import os
import copy

SUBS_DIR='/home/mikel/doctorado/subgraphs/subset/'
OUT_DIR='/home/mikel/doctorado/subgraphs/matcher-python'
#ALIGN_LIST=['NameAndPropertyAlignment', 'StringDistAlignment', 'NameEqAlignment', 'EditDistNameAlignment', 'SMOANameAlignment', 'SubsDistNameAlignment', 'JWNLAlignment']
ALIGN_LIST=['NameAndPropertyAlignment', 'NameEqAlignment', 'EditDistNameAlignment', 'SMOANameAlignment', 'SubsDistNameAlignment', 'JWNLAlignment']
POOL_SIZE=10
THRESHOLD=0.7

class MongoAlign(Document):
    source_onto = StringField(required=True)
    target_onto = StringField(required=True)
    source_obj = StringField()
    target_obj = StringField()
    align_class = StringField(required=True)
    score = FloatField(required=True)
    
class BlackList(Document):
    ontology = StringField(required=True)
    last_access = DateTimeField()
    
class AvgAlign(Document):
    source_obj = StringField(required=True)
    target_obj = StringField(required=True)
    avg_score = FloatField(required=True)


class Graph():
    def __init__(self):
        self.vertex_list = []
        self.edge_list = []
        
    def add_vertex(self, vertex):
        self.vertex_list.append(vertex)
        
    def add_edge(self, edge):
        self.edge_list.append(edge)
        
    def replace_vertex(self, old, new):
        for n, i in enumerate(self.vertex_list):
            if i.vertex_name == old:
                new_vertex = Vertex(i.vertex_id, new)
                self.vertex_list[n] = new_vertex
    
    def replace_edges(self, old, new):
        for n, i in enumerate(self.edge_list):
            if i.name == old:
                new_edge = Edge(i.source, i.target, new)
                self.edge_list[n] = new_edge

class Vertex():
    def __init__(self, vertex_id, name):
        self.vertex_id = vertex_id
        self.vertex_name = name

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
                print 'Creating alignment between %s and %s using %s' % (o1, o2, align_class)
                ap.init(o1, o2)
                ap.align()
                print 'cells', len(ap.cell_list)
                if len(ap.cell_list) == 0:
                    try:
                        mongo = MongoAlign(source_onto=o1, target_onto=o2, source_obj=None, target_obj=None, align_class=align_class, score=0.0)
                        mongo.save()
                    except Exception as e:
                        print e
                else:
                    for cell in ap.cell_list:
                        if str(cell.prop1[0]) == 'http://swrc.ontoware.org/ontology#editor' and str(cell.prop2[0]) == 'http://purl.org/ontology/bibo/editor':
                            print str(cell.prop1[0]), str(cell.prop2[0]), cell.measure
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


def process_avg(source):
    targets = MongoAlign.objects(source_obj=source).distinct(field="target_obj")
    for target in targets:
        avg_align = AvgAlign.objects(source_obj=source, target_obj=target)
        if len(avg_align) <= 0:
            results = MongoAlign.objects(source_obj=source, target_obj=target)
            total_score = 0
            for result in results:
                if result.align_class not in ['NameEqAlignment', 'StringDistAlignment']:
                    total_score += result.score
            avg_score = total_score / (len(ALIGN_LIST) - 1)
            avg_align = AvgAlign(source_obj=source, target_obj=target, avg_score=avg_score)
            avg_align.save()

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
            vertex_name = chunks[2].replace('\n', '').replace('<', '').replace('>', '')
            base = get_base(vertex_name)
            if base not in ontology_list:
                ontology_list.append(base)
            vertex = Vertex(vertex_id, vertex_name)
            graph.add_vertex(vertex)
        elif line.find('d') == 0:
            chunks = line.split(' ')
            source = chunks[1]
            target = chunks[2]
            name = chunks[3].replace('"', '').replace('<', '').replace('>', '')
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

print 'End of map...'

sources = MongoAlign.objects().distinct(field="source_obj")

print 'Calculating Averages...'

# TODO: Map
source_list = []
#print type(sources), dir(sources)
#for source in sources:
#    source_list.append(source)
    
#pool.map(process_avg, source_list)
pool.map(process_avg, sources)

for graph1 in graph_dict:
    for graph2 in graph_dict:
        if graph1 != graph2:
            new_graph1 = copy.deepcopy(graph_dict[graph1])
            new_graph2 = copy.deepcopy(graph_dict[graph2])
            for vertex1 in graph_dict[graph1].vertex_list:
                for vertex2 in graph_dict[graph2].vertex_list:
                    if vertex1.vertex_name != vertex2.vertex_name:
                        align = MongoAlign.objects(source_obj=vertex1.vertex_name, target_obj=vertex2.vertex_name, align_class='NameEqAlignment')
                        if len(align) > 0:
                            for al in align:
                                if al.score == 1:
                                    code = str(uuid.uuid4())
                                    new_graph1.replace_vertex(vertex1.vertex_name, code)
                                    new_graph2.replace_vertex(vertex2.vertex_name, code)
                                    #break
                        else:
                            align = AvgAlign.objects(source_obj=vertex1.vertex_name, target_obj=vertex2.vertex_name)
                            if len(align) > 0:
                                for al in align:
                                    if al.avg_score > THRESHOLD:
                                        code = str(uuid.uuid4())
                                        new_graph1.replace_vertex(vertex1.vertex_name, code)
                                        new_graph2.replace_vertex(vertex2.vertex_name, code)
                                        #break
            for edge1 in graph_dict[graph1].edge_list:
                for edge2 in graph_dict[graph2].edge_list:
                    if edge1.name != edge2.name:
                        align = MongoAlign.objects(source_obj=edge1.name, target_obj=edge2.name, align_class='NameEqAlignment')
                        if len(align) > 0:
                            for al in align:
                                if al.score == 1:
                                    code = str(uuid.uuid4())
                                    new_graph1.replace_edges(edge1.name, code)
                                    new_graph2.replace_edges(edge2.name, code)
                                    #break
                        else:
                            align = AvgAlign.objects(source_obj=edge1.name, target_obj=edge2.name)
                            if len(align) > 0:
                                for al in align:
                                    if al.avg_score > THRESHOLD:
                                        code = str(uuid.uuid4())
                                        new_graph1.replace_edges(edge1.name, code)
                                        new_graph2.replace_edges(edge2.name, code)
                                        #break
                                        
            
            output_dir = '%s/%s-%s/' % (OUT_DIR, graph1, graph2)
            output_file1 = '%s/%s-%s/%s' % (OUT_DIR, graph1, graph2, graph1)
            output_file2 = '%s/%s-%s/%s' % (OUT_DIR, graph1, graph2, graph2)
            
            
            os.mkdir(output_dir)
            f = open(output_file1, 'w')
            for vertex in new_graph1.vertex_list:
                f.write('v %s %s\n' % (vertex.vertex_id, vertex.vertex_name))
            for edge in new_graph1.edge_list:
                f.write('d %s %s %s\n' % (edge.source, edge.target, edge.name))
            f.close()
            f = open(output_file2, 'w')
            for vertex in new_graph2.vertex_list:
                f.write('v %s %s\n' % (vertex.vertex_id, vertex.vertex_name))
            for edge in new_graph2.edge_list:
                f.write('d %s %s %s\n' % (edge.source, edge.target, edge.name))
            f.close()
            

