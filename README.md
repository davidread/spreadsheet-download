Tools for downloading spreadsheets from data.gov.uk and gov.uk.

## data.gov.uk

Get the data:
```
wget https://data.gov.uk/data/dumps/data.gov.uk-ckan-meta-data-2017-12-03.v2.jsonl.gz
gunzip data.gov.uk-ckan-meta-data-2017-12-03.v2.jsonl.gz
mv data.gov.uk-ckan-meta-data-2017-12-03.v2.jsonl dgu.jsonl
head -n 100 dgu.jsonl >dgu.jsonl.sample
```

Install your environment:
```
pipenv install --three
mkdir dgu_data
```

Run it
```
pipenv run python download_dgu.py 
```

## gov.uk

TODO
