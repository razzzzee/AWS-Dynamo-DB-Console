using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.CredentialManagement;

namespace TestDynamoDBConnection
{
    class Program
    {
        private static Random random = new Random();
        private static readonly string[] Games = { "Super Mario Bros", "Donkey Kong", "Legend of Zelda", "Tetris" };

        static void Main(string[] args)
        {
            Console.WriteLine("Launched!");

            var sharedFile = new SharedCredentialsFile();
            // replace profile name with your own shared credentials
            sharedFile.TryGetProfile("default", out var profile);
            AWSCredentialsFactory.TryGetAWSCredentials(profile, sharedFile, out var credentials);

            AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials,Amazon.RegionEndpoint.USEast2);
            DynamoDBContext context = new DynamoDBContext(client);
            Table table = Table.LoadTable(client, "HighScores");

            bool populateTable = false;
            if (populateTable)
                PopulateDbWithTestData(table);

            // replace user after you populate your DB with random data
            string userToQuery = "CFGV";
            // query with partition key
            GetAllHighScoresForUser(userToQuery, client);
        }

        static void GetAllHighScoresForUser(string user, AmazonDynamoDBClient client)
        {
            Console.WriteLine("GetAllHighScoresForUser");
            // query only using partition key (Username)
            var request = new QueryRequest
            {
                TableName = "HighScores",
                KeyConditionExpression = "Username = :v_Id",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                                 { ":v_Id", new AttributeValue { S = user } }
                             },
                ScanIndexForward = true
            };
            var response = client.QueryAsync(request).Result;
            PrintResults(response.Items);
        }

        static void PopulateDbWithTestData(Table table)
        {
            const Int32 numUsers = 10;
            const Int32 scoreMin = 0;
            const Int32 scoreMax = 100;

            for (Int32 i = 0; i < numUsers; i++)
            {
                string userName = RandomString(4);
                foreach (string game in Games)
                {
                    Int32 score = random.Next(scoreMin, scoreMax);
                    var entry = new Document();
                    entry["Username"] = userName;
                    entry["Game"] = game;
                    entry["TopScore"] = score;
                    entry["Timestamp"] = DateTime.Now;
                    _ = table.PutItemAsync(entry).Result;
                }
            }
        }

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        static void PrintResults(List<Dictionary<string, AttributeValue>> items, bool hasTimestamp = false)
        {
            foreach (var item in items)
            {
                string s = item["Username"].S + " - " + item["Game"].S + " - " + item["TopScore"].N;
                if (hasTimestamp)
                    s += " - " + item["Timestamp"].S;
                Console.WriteLine(s);
            }
        }
    }
}
