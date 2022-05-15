import { 
  DynamoDBClient,
  PutItemCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";
import { unmarshall, marshall } from "@aws-sdk/util-dynamodb";
import faker from "@faker-js/faker";
import { v4 as uuid } from "uuid";
import { DateTime } from "luxon";

const client = new DynamoDBClient({
  region: 'us-east-1',
});

///// CONFIGURATION v
const AGGREGATION_INTERVAL_SECONDS = 3;
const TABLE_NAME = 'hw-blue-green-aggregation';
const MAX_RETRIES = 3;
///// CONFIGURATION ^

type Alert = {
  id: string,
  city: string,
}

enum AggregationStage {
  BLUE = 'blue',
  GREEN = 'green',
}

type Aggregation = {
  userId: string,
  blue: Record<string, Alert>,
  green: Record<string, Alert>,
  stage: AggregationStage,
  cutOff: string, // ISO
}

function getAlert(): Alert {
  return {
    id: uuid(),
    city: faker.address.city(),
  }
}

function getNewAggregation(userId: string, alert: Alert): Aggregation {
  return {
    userId,
    blue: { [alert.id]: alert },
    green: {},
    stage: AggregationStage.BLUE,
    cutOff: DateTime.now().plus({
      seconds: AGGREGATION_INTERVAL_SECONDS,
    }).toISO(),
  }
}

async function addAlert(userId: string, alert: Alert, retryNumber: number = 0): Promise<void> {
  const now = DateTime.now();
  const nowISO = now.toISO();
  const nextCutOffIso = now.plus({
    seconds: AGGREGATION_INTERVAL_SECONDS
  }).toISO();
  
  const newAggregation = getNewAggregation(userId, alert);
  const createNewAggregationCommand = new PutItemCommand({
    Item: marshall(newAggregation),
    TableName: TABLE_NAME,
    ConditionExpression: "attribute_not_exists(userId)",
  });

  const appendToBlueIfBeforeCutOffCommand = new UpdateItemCommand({
    Key: marshall({ userId }),
    TableName: TABLE_NAME,
    UpdateExpression: "SET #blue.#alertId = :alert",
    ConditionExpression: "attribute_exists(userId) AND attribute_not_exists(#blue.#alertId) AND #stage = :blue AND #cutOff > :now",
    ExpressionAttributeNames: {
      "#cutOff": "cutOff",
      "#blue": AggregationStage.BLUE,
      "#stage": "stage",
      "#alertId": alert.id,
    },
    ExpressionAttributeValues: marshall({
      ":now": nowISO,
      ":alert": alert,
      ":blue": AggregationStage.BLUE,
    }),
    ReturnValues: "ALL_NEW",
  })

  const appendToGreenIfBeforeCutOffCommand = new UpdateItemCommand({
    Key: marshall({ userId }),
    TableName: TABLE_NAME,
    UpdateExpression: "SET #green.#alertId = :alert",
    ConditionExpression: "attribute_exists(userId) AND attribute_not_exists(#green.#alertId) AND #stage = :green AND #cutOff > :now",
    ExpressionAttributeNames: {
      "#cutOff": "cutOff",
      "#green": AggregationStage.GREEN,
      "#stage": "stage",
      "#alertId": alert.id,
    },
    ExpressionAttributeValues: marshall({
      ":now": nowISO,
      ":alert": alert,
      ":green": AggregationStage.GREEN,
    }),
    ReturnValues: "ALL_NEW",
  })

  const switchToGreenIfAfterCutOffCommand = new UpdateItemCommand({
    Key: marshall({ userId }),
    TableName: TABLE_NAME,
    UpdateExpression: "SET #green = :alert, #blue = :empty, #stage=:green, #cutOff = :nextCutOff",
    ConditionExpression: "attribute_exists(userId) AND #stage = :blue AND #cutOff < :now",
    ExpressionAttributeNames: {
      "#cutOff": "cutOff",
      "#green": AggregationStage.GREEN,
      "#blue": AggregationStage.BLUE,
      "#stage": "stage",
    },
    ExpressionAttributeValues: marshall({
      ":now": nowISO,
      ":nextCutOff": nextCutOffIso,
      ":alert": {[alert.id]: alert},
      ":blue": AggregationStage.BLUE,
      ":green": AggregationStage.GREEN,
      ":empty": {},
    }),
    ReturnValues: "ALL_NEW",
  })

  const switchToBlueIfAfterCutOffCommand = new UpdateItemCommand({
    Key: marshall({ userId }),
    TableName: TABLE_NAME,
    UpdateExpression: "SET #blue = :alert, #green = :empty, #stage=:blue, #cutOff = :nextCutOff",
    ConditionExpression: "attribute_exists(userId) AND #stage = :green AND #cutOff < :now",
    ExpressionAttributeNames: {
      "#cutOff": "cutOff",
      "#green": AggregationStage.GREEN,
      "#blue": AggregationStage.BLUE,
      "#stage": "stage",
    },
    ExpressionAttributeValues: marshall({
      ":now": nowISO,
      ":nextCutOff": nextCutOffIso,
      ":alert": {[alert.id]: alert},
      ":green": AggregationStage.GREEN,
      ":blue": AggregationStage.BLUE,
      ":empty": {},
    }),
    ReturnValues: "ALL_NEW",
  })

  try {
    // Only one will succeed
    const results = await Promise.any([
      client.send(createNewAggregationCommand),
      client.send(appendToBlueIfBeforeCutOffCommand),
      client.send(appendToGreenIfBeforeCutOffCommand),
      client.send(switchToGreenIfAfterCutOffCommand),
      client.send(switchToBlueIfAfterCutOffCommand),
    ]);
  
    let updatedAggregation: Aggregation | null = null

    // @ts-ignore
    if (results.Attributes !== undefined) {
      updatedAggregation = unmarshall(results.Attributes) as Aggregation;
    } else {
      updatedAggregation = newAggregation
    }

    const cutOffFormatted = DateTime.fromISO(updatedAggregation.cutOff).toFormat("HH:mm:ss.SSS");
    const nowFormatted = now.toFormat("HH:mm:ss.SSS");

    console.log(`[${nowFormatted} -> ${cutOffFormatted}]: ${alert.city}`);
  } catch (err) {
    // All promises failed, this is unexpected. Let's retry
    if (retryNumber < MAX_RETRIES - 1) {
      return addAlert(userId, alert, retryNumber + 1);
    }

    console.log("\nAll requests failed:");
    (err as AggregateError).errors
      .forEach((e, index) => console.log(`> ${index + 1}. ${e.message}`));

    process.exit(1);
  }
}

const sleep = (ms: number = 1000) =>

  new Promise((res) => setTimeout(res, ms));

async function main() {
  console.log("BLUE/GREEN AGGREGATION STARTED");
  console.log("------------------------------\n");

  const userId = "c5f46a7f-60c4-4878-bb87-d9b8985b41b0";
  while(true) {
    await addAlert(userId, getAlert());

    const randomSleepDuration = (Math.random() * 900) + 100;
    await sleep(randomSleepDuration);
  }
}

main();
