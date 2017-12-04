'''
Downloads data.gov.uk Excel files
'''

import argparse
import json

from download import download
from loopstats import LoopStats


def process_jsonl(dgu_jsonl_filepath):
    loopstats = LoopStats()
    with open(dgu_jsonl_filepath, 'r') as f:
        for line in f:
            dataset = json.loads(line)
            for resource in dataset['resources']:
                res_identifier = '{}/resource/{}'.format(dataset['name'], resource['id'])
                if resource.get('format') == 'XLS':
                    print(loopstats.add('format=XLS', res_identifier))
                elif resource.get('qa', {}).get('format') == 'XLS':
                    print(loopstats.add('QA.format=XLS', res_identifier))
                else:
                    loopstats.add('rejected format={}'.format(resource.get('format')), res_identifier)
                    continue
                
                res_identifier = '{}-{}'.format(dataset['name'], resource['id'][:4])
                download(resource['url'], id_=res_identifier, extension='xls')
        loopstats.print_every_x_iterations(100)
        
    print('\nStats:\n', loopstats)
    #pool = multiprocessing.Pool(processes=4)
    #pool.map(process_url, urls)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dgu-jsonl-filepath', default='dgu.jsonl.sample')
    args = parser.parse_args()
    process_jsonl(args.dgu_jsonl_filepath)
    
