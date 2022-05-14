const AWS = require("aws-sdk");
const Converter = AWS.DynamoDB.Converter;

exports.handler = async (event) => {
  for (const record of event.Records) {
    const { NewImage, OldImage } = record.dynamodb;
    const newImage = Converter.unmarshall(NewImage);
    const oldImage = Converter.unmarshall(OldImage);

    if (oldImage.stage !== newImage.stage && oldImage[oldImage.stage]) {
      const alerts = oldImage[oldImage.stage];
      
      console.log("BATCH:", JSON.stringify({
        cutOff: new Date(oldImage.cutOff).toISOString(),
        timestamp: new Date().toISOString(),
        cities: Object.values(alerts).map((a) => a.city).sort()
      }, null, 2));
    }
  }

  return {
    ok: true
  };
};
