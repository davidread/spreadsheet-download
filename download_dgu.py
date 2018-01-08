'''
Downloads data.gov.uk Excel files
'''

import argparse
import json

import boto3

from download import download, requests_wrapper, DownloadException
from loopstats import LoopStats


S3_BUCKET_NAME = 'buod-spreadsheetclustering-dgu'

def process_jsonl(dgu_jsonl_filepath, limit, dataset_filter, res_filter, dont_download, dont_upload):
    res_stats = LoopStats()
    dataset_stats = LoopStats()

    with open(dgu_jsonl_filepath, 'r') as f:
        for line in f:
            dataset = json.loads(line)

            if dataset_filter and dataset['name'] != dataset_filter:
                continue

            dataset_has_xls = False
            for resource in dataset['resources']:
                res_identifier = '{}/resource/{}'.format(dataset['name'], resource['id'])
                print(res_identifier)
                if res_filter and res_filter not in (res_identifier, resource['id']):
                    continue
                if resource.get('format') == 'XLS':
                    print(res_stats.add('format=XLS', res_identifier))
                    dataset_has_xls = True
                elif resource.get('qa', {}).get('format') == 'XLS':
                    print(res_stats.add('QA.format=XLS', res_identifier))
                    if not dataset_has_xls:
                        dataset_has_xls = 'Has XLS by falling back to QA'
                else:
                    res_stats.add('rejected format={}'.format(resource.get('format')), res_identifier)
                    continue
                
                if dont_download:
                    break

                # download
                res_filename_identifier = '{}-{}'.format(dataset['name'], resource['id'][:4])
                try:
                    filepath, response = download(
                        resource['url'], id_=res_filename_identifier, extension='xls')
                except DownloadException as e:
                    error_type = str(e).split(':')[0]
                    error_details = str(e)[len(error_type):]
                    print(res_stats.add('Download error: {}'.format(error_type), '{} - {}'.format(error_details, res_identifier)))
                    continue

                if dont_upload:
                    break

                # upload to S3
                s3 = boto3.client('s3')
                s3.upload_file(filepath, S3_BUCKET_NAME, res_identifier)
                print(res_stats.add('uploaded ok', res_identifier))

            if dataset_has_xls is True:
                dataset_stats.add('Has XLS', dataset['name'])
            elif dataset_has_xls is False:
                dataset_stats.add('No XLS', dataset['name'])
            else:
                dataset_stats.add(dataset_has_xls, dataset['name'])
            res_stats.print_every_x_iterations(1000)
            dataset_stats.print_every_x_iterations(1000)
            if limit and dataset_stats.count >= limit:
                print('Stopping now the limit {} is reached'.format(limit))
                break

    print('\nResource Stats:\n', res_stats)
    print('\nDataset Stats:\n', dataset_stats)
    #pool = multiprocessing.Pool(processes=4)
    #pool.map(process_url, urls)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-f', '--dgu-jsonl-filepath', default='dgu.jsonl.sample')
    parser.add_argument('--limit', type=int, help='Stop after a given number of datasets')
    parser.add_argument('-d', '--dataset-filter', default=None, help='Filter to a particular dataset ID')
    parser.add_argument('-r', '--res-filter', default=None, help='Filter to a particular resource ID')
    parser.add_argument('--dont-download', action='store_true', help='Stop after identifying the resource (no download or upload to S3)')
    parser.add_argument('--dont-upload', action='store_true', help='Stop after downloading the data (no upload to S3)')
    args = parser.parse_args()

    if not args.dataset_filter and args.res_filter and '/' in args.res_filter:
        args.dataset_filter = args.res_filter.split('/')[0]

    process_jsonl(args.dgu_jsonl_filepath, args.limit, args.dataset_filter, args.res_filter, args.dont_download, args.dont_upload)
