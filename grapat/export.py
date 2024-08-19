import json
import os.path
import re
import sys
import time

from lxml import etree

from grapat.db import db_fetch_results

map_node_type = {
    'node_type_proponent': 'pro',
    'node_type_opponent': 'opp',
    'node_type_mixed': 'mixed'
}

map_edge_type = {
    'support': 'sup',
    'support_by_example': 'exa',
    'rebut': 'reb',
    'undercut': 'und',
    'additional_source': 'add'
}


def sorted_nicely(l):
    """
    Sorts the given iterable in the way that is expected.
    http://stackoverflow.com/a/2669120
    """

    def convert(text):
        return int(text) if text.isdigit() else text

    def alphanum_key(key):
        return [convert(c) for c in re.split('([0-9]+)', key)]

    return sorted(l, key=alphanum_key)


def query_latest_annotation(text_id, username):
    results = db_fetch_results(
        "SELECT graph, time FROM results WHERE username=? AND annotation_bundle=?",
        (username, text_id)
    )
    if results:
        results.sort(key=lambda row: row[1], reverse=True)
        graph, _ = results[0]
        graph = json.loads(graph)
    else:
        graph = None
    return graph


def read_edus_from_source_xml(text_id, sentence):
    annotation_bundle = db_fetch_results(
        'SELECT id, semantics, entity_id, segment FROM annotation_bundles WHERE id=? AND entity_id=?',
        (text_id, sentence))
    edus = {str(edu_id): edu[3] for edu_id, edu in enumerate(annotation_bundle)}
    return edus


def graph_to_xml(text_id, graph, edus):
    doc_elm = etree.XML('<arggraph id="%s" />' % text_id)

    # a mapping from node_ids to xml_ids
    node_to_xml_ids = {}

    # serialize edus
    max_edu_id = 1
    for edu_id in sorted_nicely(list(edus.keys())):
        edu_text = edus[edu_id]
        new_edu_id = 'e%d' % max_edu_id
        node_to_xml_ids['word_%s' % edu_id] = new_edu_id
        max_edu_id += 1
        edu_elm = etree.XML('<edu id="%s" />' % new_edu_id)
        edu_elm.text = etree.CDATA(edu_text)
        doc_elm.append(edu_elm)

    # serialize joints
    max_joint_id = 1
    for k in sorted_nicely(graph['nodes'].keys()):
        v = graph['nodes'][k]
        if 'n_type' in v and v['n_type'] == 'node_type_edu_join':
            joint_id = 'j%d' % max_joint_id
            node_to_xml_ids[k] = joint_id
            max_joint_id += 1
            joint_elm = etree.XML('<joint id="%s" />' % joint_id)
            doc_elm.append(joint_elm)

    # serialize adus
    max_adu_id = 1
    for k in sorted_nicely(graph['nodes'].keys()):
        v = graph['nodes'][k]
        if v.get('n_type', None) in ['node_type_proponent',
                                     'node_type_opponent',
                                     'node_type_mixed']:
            adu_type = map_node_type[v['n_type']]
            adu_id = 'a%d' % max_adu_id
            node_to_xml_ids[k] = adu_id
            max_adu_id += 1
            adu_elm = etree.XML('<adu id="%s" type="%s" />' % (adu_id, adu_type))
            doc_elm.append(adu_elm)

    # serialize edges
    to_edu_edges = []
    to_joint_edges = []
    to_adu_edges = []
    edge_to_xml_id = {}
    max_edge_id = 1

    def is_garbage_edge(source, attrib):
        return (attrib.get('c_type', None) is None and
                not source.startswith('word_'))

    def iter_edges(graph):
        for source in sorted_nicely(graph['edges'].keys()):
            for target in sorted_nicely(graph['edges'][source].keys()):
                for conn_id in sorted_nicely(graph['edges'][source][target].keys()):
                    attrib = graph['edges'][source][target][conn_id]
                    if not is_garbage_edge(source, attrib):
                        yield source, target, conn_id, attrib

    # first pass: register edges and relation nodes, skip garbage edges
    for source, target, conn_id, attrib in iter_edges(graph):
        edge_id = 'c%d' % max_edge_id
        max_edge_id += 1
        edge_to_xml_id[conn_id] = edge_id
        if attrib.get('label_node_id', None) is not None:
            node_to_xml_ids[attrib['label_node_id']] = edge_id

    # second pass: generate xml elements
    for source, target, conn_id, attrib in iter_edges(graph):
        edge_id = edge_to_xml_id[conn_id]
        source_id = node_to_xml_ids[source]
        target_id = node_to_xml_ids[target]
        if source.startswith('word_'):
            edge_elm = etree.XML(
                '<edge id="%s" src="%s" trg="%s" type="seg" />' % (
                    edge_id, source_id, target_id))
            to_edu_edges.append(edge_elm)
        elif node_to_xml_ids[source].startswith('j'):
            edge_elm = etree.XML(
                '<edge id="%s" src="%s" trg="%s" type="seg" />' % (
                    edge_id, source_id, target_id))
            to_joint_edges.append(edge_elm)
        else:
            edge_elm = etree.XML(
                '<edge id="%s" src="%s" trg="%s" type="%s" />' % (
                    edge_id, source_id, target_id,
                    map_edge_type[attrib['c_type']]))
            to_adu_edges.append(edge_elm)

    # serialize in order
    for e in to_edu_edges:
        doc_elm.append(e)
    for j in to_joint_edges:
        doc_elm.append(j)
    for a in to_adu_edges:
        doc_elm.append(a)

    # write xml
    xml_string = etree.tostring(
        doc_elm, encoding='UTF-8', pretty_print=True, xml_declaration=True)
    return xml_string


def get_xml_from_grapat(username, text_id, sentence):
    graph = query_latest_annotation(text_id, username)
    if graph is None:
        return
    try:
        edus = read_edus_from_source_xml(text_id, sentence)
    except IOError:
        return
    return graph_to_xml(text_id, graph, edus)


def export_file(text_id):
    db_res = db_fetch_results("SELECT DISTINCT username, sentence FROM results WHERE annotation_bundle LIKE ?",
                              params=(text_id,))
    username, sentence = db_res[0]
    return get_xml_from_grapat(username, text_id, sentence)


def export_db():
    timestr = time.strftime("%Y%m%d-%H%M%S")
    export_path = os.path.join("exports", timestr)
    os.makedirs(export_path, exist_ok=True)
    os.chmod(export_path, 0o777)
    for username, text_id, sentence in db_fetch_results(
            "SELECT DISTINCT username, annotation_bundle, sentence FROM results"):
        try:
            graph_xml = get_xml_from_grapat(username, text_id, sentence)
            filename = f'{text_id}-{sentence}-{username}.xml'
            with open(os.path.join(export_path, filename), 'wb') as fh:
                fh.write(graph_xml)
            os.chmod(filename, 0o777)
        except KeyError as e:
            print(f"Error while exporting file {text_id}", e, file=sys.stderr)
            continue
