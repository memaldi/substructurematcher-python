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

SUBS_DIR='/home/mikel/doctorado/subgraphs/subs/'
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

def get_base(uri):
    if '#' in uri:
        return uri.split('#')[0].replace('<', '') + '#'
    else:
        chunks = uri.split('/')
        base = ''
        for i in xrange(len(chunks) - 1):
            base += chunks[i] + '/'
        return base.replace('<', '')
        

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

for graph1 in graph_dict:
    for graph2 in graph_dict:
        if graph1 != graph2:
            print graph1, graph2
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
                            align = MongoAlign.objects(source_obj=vertex1.vertex_name, target_obj=vertex2.vertex_name)
                            if len(align) > 0:
                                avg_score = 0
                                for al in align:
                                    if al.align_class != 'NameEqAlignment':
                                        avg_score += al.score
                                avg_score = avg_score / (len(ALIGN_LIST) - 1)                               
                                if avg_score > THRESHOLD:
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
                f.write('d %s %s %s\n' % (edge.source, edge.target, edge.name.replace('\n', '')))
            f.close()
            f = open(output_file2, 'w')
            for vertex in new_graph2.vertex_list:
                f.write('v %s %s\n' % (vertex.vertex_id, vertex.vertex_name))
            for edge in new_graph2.edge_list:
                f.write('d %s %s %s\n' % (edge.source, edge.target, edge.name.replace('\n', '')))
            f.close()
