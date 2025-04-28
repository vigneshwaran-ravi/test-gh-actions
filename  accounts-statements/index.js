import moment from "moment";
import { Connection } from "postgresql-client";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  TransactWriteCommand,
  BatchWriteCommand,
  GetCommand,
  QueryCommand,
  UpdateCommand,
  DeleteCommand,
  ScanCommand,
} from "@aws-sdk/lib-dynamodb";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const lambdaClient = new LambdaClient();
const sqsClient = new SQSClient();
const s3Client = new S3Client();

const dynamoClient = new DynamoDBClient();
const marshallOptions = {
  // Whether to automatically convert empty strings, blobs, and sets to `null`.
  convertEmptyValues: false, // false, by default.
  // Whether to remove undefined values while marshalling.
  removeUndefinedValues: true, // false, by default.
  // Whether to convert typeof object to map attribute.
  convertClassInstanceToMap: false, // false, by default.
};
const unmarshallOptions = {
  // Whether to return numbers as a string instead of converting them to native JavaScript numbers.
  wrapNumbers: false, // false, by default.
};
const translateConfig = { marshallOptions, unmarshallOptions };
const docClient = DynamoDBDocumentClient.from(dynamoClient, translateConfig);

function GpException(code, message) {
  this.message = message;
  this.code = code;
}

const connectionString = process.env.PG_CONNECTION_STRING;

const DEFAULT_PAGESIZE = 100;
const oneDay = 24 * 60 * 60;
const oneHour = 60 * 60;

const bucket_name = "adjusterx-store";

export const handler = async (event, context, callback) => {
  const done = (code, body) =>
    callback(null, {
      statusCode: code,
      body: JSON.stringify(body),
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });

  let response;
  console.log("***** event: ", event);

  if (event.resource) {
    console.log(event.requestContext.authorizer?.claims);
    const userId = event.requestContext.authorizer?.claims["custom:user_id"];
    const isAdmin =
      event.requestContext.authorizer?.claims["cognito:groups"] &&
      event.requestContext.authorizer?.claims["cognito:groups"]?.includes(
        "Admin"
      );
    const userFullName = event.requestContext.authorizer?.claims?.name;
    const userEmail = event.requestContext.authorizer?.claims?.email;
    console.log("userId: ", userId + " " + isAdmin);
    try {
      switch (event.resource) {
        case "/my/statements/generate":
          if (event.httpMethod === "POST") {
            let request = JSON.parse(event.body);
            response = await generate(userId, request?.year, request?.month);
          }
          break;
        case "/my/statements":
          if (event.httpMethod === "GET") {
            response = await getAccountStatements(
              userId,
              event.queryStringParameters.year,
              event.queryStringParameters.month
            );
          }
          break;
      }
    } catch (error) {
      let statusCode = 500;
      if (Number.isInteger(error.code)) {
        statusCode = error.code;
      }
      done(statusCode, {
        success: false,
        content: error.message,
      });
    }

    if (response) {
      done(200, {
        success: true,
        content: response,
      });
    } else {
      done(404, {
        success: false,
        content: "Not Found.",
      });
    }
  } else if (event.Records) {
    for (const record of event.Records) {
      const { eventSourceARN } = record;
      const { body } = record;
      // console.log("*** event-record-body::", JSON.stringify(body));
      const jsonObject = JSON.parse(body);

      switch (eventSourceARN) {
        case process.env.SQS_ARN_Account_Statements:
          if (jsonObject.action === "sendUserStatement") {
            await sendUserStatement(jsonObject.userId, jsonObject.year);
            break;
          }
          response = {};
          break;
      }
    }
  } else if (event.action) {
    try {
      switch (event.action) {
        case "sendUserStatement":
          response = await sendUserStatement(event.userId, event.year);
          break;
        case "sendAllUsersStatements":
          response = await sendAllUsersStatements(event.year);
          break;
      }

      if (response) {
        return {
          success: true,
          content: response,
        };
      } else {
        return {
          success: false,
          errors: [{ code: "GENE-0002" }],
        };
      }
    } catch (error) {
      return {
        success: false,
        content: error.message,
      };
    }
  }
};

async function sendAllUsersStatements(year) {
  console.log("sendAllUsersStatements: ", year);

  let fromDate = new Date(year, 0, 0);
  console.log(fromDate);
  const allUsers = await _getAllUsersWithActivity(
    Math.round(fromDate.getTime() / 1000)
  );
  console.log(allUsers?.length);
  for (let user of allUsers) {
    const userId = user.userId;
    console.log(userId);
    await createSQSMessage(process.env.SQS_URL_Account_Statements, {
      action: "sendUserStatement",
      userId,
      year,
    });
  }
  return allUsers?.length;
}

async function sendUserStatement(userId, year) {
  console.log("sendUserStatement: " + userId + ", " + year);
  let keyFile = await generate(userId, year);
  let user = await _getUserById(userId);

  let attachments = [
    {
      name: keyFile.substring(keyFile.lastIndexOf("/") + 1),
      s3: {
        bucket: bucket_name,
        key: keyFile,
      },
    },
  ];

  console.log("attachments: ", attachments);
  let to = [
    {
      email: user?.email,
      name: user?.name,
    },
  ];
  await createSQSSendNotification(
    "sendEmailAnnualStatement",
    to,
    { year, nameUser: user?.name },
    attachments
  );
  return "done";
}

async function generate(userId, year, month) {
  console.log("generate: " + userId + ", year: " + year + ", month: " + month);
  if (!userId) {
    throw new GpException(400, "Field `userId` is required");
  }
  if (!year) {
    throw new GpException(400, "Field `year` is required");
  }
  if (year < 2020 || year > 3000) {
    throw new GpException(400, "Invalid `year` value");
  }

  let user = await _getUserById(userId);
  let stripeId = await _getStripeIdByUserId(userId);

  const now = Math.round(Date.now() / 1000);

  let from = undefined;
  let to = undefined;
  let totalYearSubscription = 0;
  let totalYearSketch = 0;
  let totalYearOther = 0;
  let transactionsPerMonth = [];

  for (let i = 0; i < 12; i++) {
    let totalMonthSubscription = 0;
    let totalMonthSketch = 0;
    let totalMonthOther = 0;

    let fromDate = new Date(year, i, 1);
    let toDate = new Date(year, i + 1, 0);
    from = Math.round(fromDate.getTime() / 1000);
    to = Math.round(toDate.getTime() / 1000);

    let payments = await _callLambdaFunction("stripe-integration", {
      action: "listPayments",
      stripeId,
      from,
      to,
    });

    payments = payments.sort((a, b) => a.createdAt - b.createdAt);

    let orders = await getAllOrders(userId, from, to);
    let ordersDic = [];
    for (let order of orders) {
      ordersDic[order.orderNumber] = order;
    }

    for (let payment of payments) {
      let pos = payment.descriptor.indexOf("SKETCHX:");
      if (pos > -1 && payment.descriptor.length >= pos + 8 + 9) {
        let orderNumber = payment.descriptor.substring(pos + 8, pos + 8 + 9);
        let order = ordersDic[orderNumber];
        if (order) {
          payment.orderNumber = orderNumber;
          payment.address =
            order.location?.address1 +
            ", " +
            (order.location?.address2 ? order.location?.address2 + ", " : "") +
            order.location?.city +
            ", " +
            order.location?.stateCode +
            " " +
            order.location?.zipcode;
        }

        totalMonthSketch += payment.amount - payment.amountRefunded;
      } else if (payment.descriptor?.includes("ROUTEXPRO-")) {
        totalMonthSubscription += payment.amount - payment.amountRefunded;
      } else if (payment.descriptor?.includes("ROUTEX-")) {
        totalMonthSubscription += payment.amount - payment.amountRefunded;
      } else {
        totalMonthOther += payment.amount - payment.amountRefunded;
      }
    }

    transactionsPerMonth.push({
      month: i,
      totalSubscription: totalMonthSubscription,
      totalSketch: totalMonthSketch,
      totalOther: totalMonthOther,
      transactions: payments,
    });

    totalYearSubscription += totalMonthSubscription;
    totalYearSketch += totalMonthSketch;
    totalYearOther += totalMonthOther;
  }

  const formatterCurrency = new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

  const formatterDecimal = new Intl.NumberFormat("en-US", {
    style: "decimal",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

  let jasperListFields = [];

  let reportRequest = {
    jasperFileName: "AnnualAccountStatement",
    environmentName: "prod",
    jasperParameters: {
      customer: user?.name,
      email: user?.email,
      phone: user?.phone,
      totalSubscription: formatterDecimal.format(totalYearSubscription),
      totalSketch: formatterDecimal.format(totalYearSketch),
      totalOther: formatterDecimal.format(totalYearOther),
      total: formatterDecimal.format(
        totalYearSubscription + totalYearSketch + totalYearOther
      ),
      year: year + "",
      today: moment(now * 1000).format("MMMM Do YYYY"),
    },
    jasperListFields,
  };

  for (let monthItem of transactionsPerMonth) {
    for (let item of monthItem.transactions) {
      jasperListFields.push({
        month: monthsList[monthItem.month],
        totalMonthlySubscription: monthItem.totalSubscription,
        totalMonthlySketch: monthItem.totalSketch,
        totalMonthlyOther: monthItem.totalOther,
        totalMonthly: formatterDecimal.format(
          monthItem.totalSubscription +
            monthItem.totalSketch +
            monthItem.totalOther
        ),
        refunded: item.refunded + "",
        amount: formatterDecimal.format(item.amount),
        amountRefunded: formatterDecimal.format(item.amountRefunded),
        totalLine: formatterDecimal.format(item.amount - item.amountRefunded),
        orderNumber: item.orderNumber || "",
        address: item.address,
        descriptor: item.address || item.descriptor,
        date: moment(item.createdAt * 1000).format("MM/DD/YYYY"),
      });
    }
  }

  let base64PDF = await _callLambdaFunction(
    "jasper-reports",
    reportRequest,
    true
  );
  const keyFile = `users/${userId}/statements/Annual_Statement_${year}.pdf`;
  await saveToS3(keyFile, base64PDF);
  return keyFile;
}

async function saveToS3(keyFile, base64PDF) {
  const buffer = Buffer.from(base64PDF, "base64");
  const input = {
    Body: buffer,
    Bucket: bucket_name,
    Key: keyFile,
  };
  const command = new PutObjectCommand(input);
  try {
    const response = await s3Client.send(command);
    return keyFile;
  } catch (err) {
    console.log(err);
    throw new GpException(500, "Error saving file");
  }
}

async function getAllOrders(userId, from, to) {
  if (!userId) {
    throw new GpException(400, "Missing parameter: userId");
  }

  let paramsSearch = {
    TableName: "jobs",
    IndexName: "UserCreatedAtIndex",
    KeyConditionExpression: "#pk = :pk AND #sk BETWEEN :from AND :to",
    // FilterExpression: "#st = :st",
    ExpressionAttributeNames: {
      "#pk": "userId",
      "#sk": "createdAt",
      // "#st": "status",
      "#loc": "location",
    },
    ExpressionAttributeValues: {
      ":pk": userId,
      ":from": from,
      ":to": to,
      // ":st": "Completed",
    },
    ProjectionExpression: "orderNumber,client,#loc, address",
  };
  try {
    const dynamoCommand = new QueryCommand(paramsSearch);
    const resultDb = await docClient.send(dynamoCommand);
    return resultDb.Items;
  } catch (err) {
    console.log("ERROR getAllOrders: ", err);
    throw new GpException(500, "Error getting orders");
  }
}

async function _getStripeIdByUserId(userId) {
  if (!userId) {
    throw new GpException(400, "Field `userId` is required");
  }

  const user = await _getUserById(userId);
  return user?.stripeId;
}

async function _getUserById(userId) {
  if (!userId) {
    throw new GpException(400, "Field `userId` is required");
  }
  if (userId.startsWith("usr_")) {
    userId = userId.slice(4);
  }

  const connection = new Connection(connectionString);
  await connection.connect();
  const result = await connection.query("select * from users where id = $1", {
    params: [userId],
  });
  await connection.close();
  if (result.rows.length === 1) {
    return _convertUser(result.fields, result.rows[0]);
  }

  return undefined;
}

async function _getAllUsersWithActivity(lastLoginFrom) {
  if (!lastLoginFrom) {
    throw new GpException(400, "Field `lastLoginFrom` is required");
  }

  const connection = new Connection(connectionString);
  await connection.connect();
  const result = await connection.query(
    "select id from users where last_login >= $1",
    { fetchCount: 10000, params: [lastLoginFrom] }
  );
  await connection.close();
  let list = [];
  if (result.rows.length > 0) {
    for (let item of result.rows) {
      list.push(_convertUser(result.fields, item));
    }
  }

  return list;
}

function _convertUser(fields, row) {
  let user = {};
  let i = 0;
  for (const field of fields) {
    if (field.fieldName === "id") {
      user["userId"] = "usr_" + row[i];
    } else {
      let fieldName = _renameField(field.fieldName);
      user[fieldName] = row[i] == null ? undefined : row[i];
    }

    i++;
  }
  return user;
}

function _renameField(fieldName) {
  const index = fieldName.indexOf("_");
  if (index > -1 && index < fieldName.length - 1) {
    fieldName =
      fieldName.substring(0, index) +
      fieldName[index + 1].toUpperCase() +
      fieldName.slice(index + 2);
    return _renameField(fieldName);
  }

  return fieldName;
}

async function _callLambdaFunction(lambdaName, payload, fullResponse) {
  console.log("_callLambdaFunction (" + lambdaName + "): ", payload);
  var params = {
    FunctionName: lambdaName, // the lambda function we are going to invoke
    InvocationType: "RequestResponse",
    LogType: "Tail",
    Payload: JSON.stringify(payload),
  };

  const command = new InvokeCommand(params);
  let response = await lambdaClient.send(command);
  response = new TextDecoder().decode(response.Payload);
  response = JSON.parse(response);
  if (fullResponse) {
    return response;
  }
  if (!response?.success) {
    console.log("ERROR _callLambdaFunction: " + lambdaName, response);
    throw new GpException(500, "Internal Error");
  } else {
    return response.content;
  }
}

async function createSQSSendNotification(action, to, bodyFields, attachments) {
  try {
    const message = {
      action,
      to,
      bodyFields,
      attachments,
    };

    const command = new SendMessageCommand({
      MessageBody: JSON.stringify(message),
      QueueUrl: process.env.SQS_URL_Email_Account_Statements,
    });
    let response = await sqsClient.send(command);

    console.log("Message put on queue emails_account_statements");
  } catch (e) {
    console.log("Exception on queue emails_account_statements", e);
    throw new GpException(500, "Internal Error");
  }
}

async function createSQSMessage(queueUrl, message) {
  try {
    const command = new SendMessageCommand({
      MessageBody: JSON.stringify(message),
      QueueUrl: queueUrl,
    });
    let response = await sqsClient.send(command);

    console.log("Message put on queue: " + queueUrl);
  } catch (e) {
    console.log("Exception on queue: " + queueUrl, e);
    throw new GpException(500, "Internal Error");
  }
}

const monthsList = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];
