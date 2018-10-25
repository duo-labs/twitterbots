import argparse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from find_amplification_bots import TweetNode, AccountNode, Edge

DEFAULT_GRAPH_FILENAME = 'amp_search_crawl.gexf'
DEFAULT_DB_PATH = 'amplification.db'


def write_graph(session, args):
    """Writes the entries in the database to a GEXF file

    Arguments:
        session {sqlalchemy.orm.Session} -- The database session
        graph_filename {str} -- The filename to write the graph to
    """

    date_format = "%Y-%m-%dT%H:%M:%S"

    with open(args.graph_file, 'w') as graph:
        graph.write(
            "<?xml version='1.0' encoding='utf-8'?>"
            "<gexf version=\"1.2\" xmlns=\"http://www.gexf.net/1.2draft\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.w3.org/2001/XMLSchema-instance\">"
            "<graph defaultedgetype=\"directed\" mode=\"dynamic\" timeformat=\"datetime\" name=\"\">\n")
        # Write the nodes
        graph.write('<nodes>\n')
        for node_class in [TweetNode, AccountNode]:
            for node in session.query(node_class).yield_per(1000):
                graph.write('<node id="{}" label="{}" start="{}" />\n'.format(
                    node.id, node.screen_name,
                    node.date_crawled.strftime(date_format)))
        graph.write('</nodes>\n')

        graph.write('<edges>\n')

        query = session.query(Edge)
        for edge in query.yield_per(1000):
            graph.write('<edge id="{}" source="{}" target="{}" />\n'.format(
                edge.id, edge.source.id, edge.target.id))
        graph.write('</edges>\n')
        graph.write('</graph>\n')
        graph.write('</gexf>\n')


def parse_args():
    """"""

    parser = argparse.ArgumentParser(
        description="Assembles graph of tweets and accounts")
    parser.add_argument('--database',
                        type=str,
                        help="path to sqlite database",
                        default=DEFAULT_DB_PATH)
    parser.add_argument(
        '--graph-file',
        '-g',
        type=str,
        help='Filename for the output GEXF graph',
        default=DEFAULT_GRAPH_FILENAME)
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = parse_args()
    database_path = args.database
    engine = create_engine('sqlite:///{}'.format(database_path))
    session_factory = sessionmaker(bind=engine)
    session = scoped_session(session_factory)
    write_graph(session, args)
