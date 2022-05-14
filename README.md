# hw-blue-green-aggregator
 
1. Create a dynamodb table on your AWS account. Key must be "userId" of type string.
2. Create a lambda function called `hw-blue-green-stream`. From AWS lambda console, paste the code from `stream.js` into the code editor and then click "deploy".
3. In order to enable your lambda function to work with streams, give it the following permissions: dynamodb:GetShardIterator, dynamodb:DescribeStream, dynamodb:GetRecords, dynamodb:ListStreams
4. Go back to your DynamoDB table and enable streams. Add a trigger pointing to the lambda created on step 2.
5. Open a terminal window on this project and run `npm install`.
6. Now run `npm start`.
7. Open another terminal window and install awslogs with `brew install awslogs`.
8. To see aggregations output, run `awslogs get /aws/lambda/hw-blue-green-dispatcher ALL --watch --query=message --filter-pattern="BATCH"`. Replace the log group `/aws/lambda/hw-blue-green-dispatcher` in case you gave a different name to the lambda created on step 2.

Extra:
Open an additional terminal, and run a secondary producer with `npm start`. Notice that cutoff dates will always be the same between both terminals because they are sync by dynamodb.
