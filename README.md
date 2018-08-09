# Twitter Bot Analysis

This project contains the files used in the Black Hat [_"Don't @ Me: Hunting Twitter Bots at Scale"_](https://duo.com/blog/dont-me-hunting-twitter-bots-at-scale) presentation to discover and aggregate a large sample of Twitter profile and tweet information.

## Setup 

### Account and Tweet Collection

_Note: We're working on creating Docker containers to automate this setup - Stay tuned!_

The data gathering is split into two scripts: `collect.py` which collects accounts, and `collect_tweets.py` which collects tweets from previously gathered accounts.

The first step is to install the requirements:

```
pip install -r requirements.txt
```

The next step is to [create a Twitter application](https://apps.twitter.com/) (detailed instructions coming soon!). After creating the application and generating an access token and secret, you can set the following environment variables:

```
export TWEEPY_CONSUMER_KEY="your_consumer_key"
export TWEEPY_CONSUMER_SECRET="your_consumer_secret"
export TWEEPY_ACCESS_TOKEN="your_access_token"
export TWEEPY_ACCESS_TOKEN_SECRET="your_access_token_secret"
```

Once these environment variables are set, you can set up the database. We encourage setting up an instance of MySQL for storing data. Once this is created, you can set the connection string as an environment variable:

```
export DB_PATH="path_to_mysql"
```

Finally, you'll want to set up a Redis instance for caching. After this is done, you're ready to start gathering data!

### Feature Extraction
The code associated with feature extraction utilizes Spark and is written in Scala; therefore, you'll need to install both. Also, if you'd like to run this code outside of an IDE (e.g., IntelliJ or Eclipse), you'll also need to install [sbt](https://www.scala-sbt.org/index.html).

After that initial setup, you should be able to assemble a jar by merely running `assembly` from the sbt interactive shell. The output will be `twitter-bots.jar`.

## Usage

### Tweet and Account Collection
The first iteration of the tool is designed to make it as easy as possible to control what types of account/tweet enumeration you want to do.

At a high level, there are two types of account enumeration:

* `enum` - This process takes `--min-id` and `--max-id` flags and looks up accounts in bulk using Twitter's `users/lookup` API endpoint. If you want a random sampling of accounts between the min and max ID's, you can set the `--enum-percentage` flag.

* `stream` - This process takes a `--stream-query` parameter if you want to filter the Twitter stream by a keyword. If this isn't provided, the sample endpoint will be used.

This is the usage for the account collection script:

```
$ python collect.py -h
usage: collect.py [-h] [--max-id MAX_ID] [--min-id MIN_ID] [--db DB]
                  [--enum-percentage ENUM_PERCENTAGE] [--no-stream]
                  [--no-enum] [--stream-query STREAM_QUERY]
                  [--account-filename ACCOUNT_FILENAME] [--stdout]

Enumerate public Twitter profiles and tweets

optional arguments:
  -h, --help            show this help message and exit
  --max-id MAX_ID       Max Twitter ID to use for enumeration
  --min-id MIN_ID       Minimum ID to use for enumeration
  --enum-percentage ENUM_PERCENTAGE, -p ENUM_PERCENTAGE
                        The percentage of 32bit account space to enumerate
                        (0-100).
  --no-stream           Disable the streaming
  --no-enum             Disable the account id enumeration
  --stream-query STREAM_QUERY, -q STREAM_QUERY
                        The query to use when streaming results
  --account-filename ACCOUNT_FILENAME, -af ACCOUNT_FILENAME
                        The filename to store compressed account JSON data
  --stdout              Print JSON to stdout instead of a file
```

After gathering accounts, you can start gathering tweets. Here's the usage for `collect_tweets.py`:

```
$ python collect_tweets.py -h
usage: collect_tweets.py [-h] [--min-tweets MIN_TWEETS]
                         [--tweet-filename TWEET_FILENAME] [--no-lookup]
                         [--stdout]

Enumerate public Twitter tweets from discovered accounts

optional arguments:
  -h, --help            show this help message and exit
  --min-tweets MIN_TWEETS, -mt MIN_TWEETS
                        The minimum number of tweets needed before fetching
                        the tweets
  --tweet-filename TWEET_FILENAME, -tf TWEET_FILENAME
                        The filename to store compressed tweet JSON data
  --no-lookup           Disable looking up users found in tweets
  --stdout              Print JSON to stdout instead of a file
```

#### Note on Running the Scripts

The `collect.py` and `collect_tweets.py` scripts can be run at the same time. It's recommended to start by running `collect.py`, since this starts populating the database with accounts. Then, `collect_tweets.py` can fetch accounts from the database, and grab the tweets.

A good solution for running both the scripts together is through something like `screen`.

#### Output Formats

The database created is mainly used as intermediary storage. It holds metadata about accounts, as well as whether or not tweets have been fetched for it.

The real value is in the [ndjson](http://ndjson.org/) files created. The `collect.py` creates a file, `accounts.json.gz` that contains the compressed JSON account information with one record on each line. Likewise, the `collect_tweets.py` script creates a file, `tweets.json.gz` that contains the compressed JSON tweet information.

### Feature Extraction

This is the usage for the feature extraction script: 

```
Performs feature extraction on twitter account and tweet data and saves data to local file system or S3.

Usage: spark-submit --class ExtractionApp twitter-bots.jar --account-data <account_data_path> --tweet-data <tweet_data_path>
       --destination <output_path> --extraction-type [unknown|training]

  -a, --account-data  <arg>      Twitter account data
  -d, --destination  <arg>       Destination for extracted data
  -e, --extraction-type  <arg>   Extracting Cresci data (training) or outputs
                                 from other scripts (unknown)
  -t, --tweet-data  <arg>        Tweet data
      --help                     Show help message
```

The files that are expected when `--extraction-type unknown` are JSON for both the account and tweet data. When `--extraction-type training`, the file formats expected are csv and parquet respectively. 

### Crawling Social Networks

We've included a script `crawl_networks.py` that makes it easy to crawl the social network for an account and generate a GEXF file that can imported into a tool like [Gephi](https://gephi.org/) for visualization.

Basic usage is as simple as `python crawl_network.py jw_sec`, where `jw_sec` is the account you want to crawl.

```
$ python crawl_network.py -h
usage: crawl_network.py [-h] [--graph-file GRAPH_FILE] [--raw RAW]
                        [--degree DEGREE] [--max-connections MAX_CONNECTIONS]
                        user

Crawl a Twitter user's social network

positional arguments:
  user                  User screen name to crawl

optional arguments:
  -h, --help            show this help message and exit
  --graph-file GRAPH_FILE, -g GRAPH_FILE
                        Filename for the output GEXF graph
  --raw RAW, -r RAW     Filename for the raw JSON data
  --degree DEGREE, -d DEGREE
                        Max degree of crawl
  --max-connections MAX_CONNECTIONS, -c MAX_CONNECTIONS
                        Max number of connections per account to crawl
```

_Note: Social networks get large very quickly, and can take quite a while to crawl. We recommend setting the `degree` and `max-connections` flags to reasonable values (such as a 1-degree crawl with 1000 max connections per account) depending on your goals and available time._

## Helpful Hints

The outputs of the tweet and account collection are very large files if run for an extended period of time; so, make sure that you have a cluster or single machine with the appropriate amount of available memory when running the feature extraction code.