### What this program does
- It runs a telegram bot.
- It connects to a MongoDB instance.
- It populates the database with [data](env-rpl-test-attempt/aggregate_payments_test/data) if it's not populated.
- The bot accepts user input in the format provided in response to the /help command.
- The program makes aggregations of data from the db based on the input.
- The bot sends the result of the aggregation.

## How to run it
- Create a telegram bot via BotFather ([instruction](https://core.telegram.org/bots/features#botfather)) and get the bot token from it.
- Install MongoDB.
- Run ```mongod --dbpath '<path>'``` from the command line where <path> is a path where you want mongod instance to store its data.
- Clone this repository.
- Run ```python main.py -h``` to see what command line arguments are accepted. 

## Dependencies
- Python 3.11
- libraries: aiohttp, pymongo, bson
- MongoDB 7.0.9 (lower will likely do, too)
