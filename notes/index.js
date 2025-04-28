import { v4 as uuidv4 } from "uuid";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  TransactWriteCommand,
  GetCommand,
  QueryCommand,
  UpdateCommand,
  DeleteCommand,
} from "@aws-sdk/lib-dynamodb";

const sqsClient = new SQSClient();
const lambdaClient = new LambdaClient();
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

export const handler = async (event, context, callback) => {
  console.log("event: ", event);
  console.log("hello world: ", event);
  console.log("hello world: ", event);
  const done = (err, res) =>
    callback(null, {
      statusCode: err ? "400" : "200",
      body: err ? err.message : JSON.stringify(res),
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });

  let response;
  if (event.resource) {
    const userId =
      event.requestContext.authorizer.userId ||
      event.requestContext.authorizer.claims["custom:user_id"];
    console.log(userId);
    console.log("resource: ", event.resource);
    switch (event.resource) {
      case "/notes":
        if (event.httpMethod === "POST") {
          response = await create(
            userId,
            JSON.parse(event.body).claimId,
            JSON.parse(event.body)
          );
        }
        if (event.httpMethod === "GET") {
          response = await getAllByUserId(
            userId,
            event.queryStringParameters.processed,
            event.queryStringParameters.type
          );
        }
        break;
      case "/notes/byClaim/{claimId+}":
        if (event.httpMethod === "GET") {
          response = await listByClaim(event.pathParameters.claimId);
        }
        break;
      case "/notes/{noteId}":
        if (event.httpMethod === "GET") {
          response = await getById(userId, event.pathParameters.noteId);
        }
        if (event.httpMethod === "PUT") {
          response = await update(
            userId,
            event.pathParameters.noteId,
            JSON.parse(event.body)
          );
        }
        break;
      case "/notes/{noteId}/processed":
        if (event.httpMethod === "PUT") {
          response = await updateProcessed(
            userId,
            event.pathParameters.noteId,
            JSON.parse(event.body)
          );
        }
        break;
      case "/claims/{claimId}/notes": // Claims X Pro
        if (event.httpMethod === "GET") {
          response = await listByClaim(event.pathParameters.claimId);
        }
        if (event.httpMethod === "POST") {
          response = await create(
            userId,
            event.pathParameters.claimId,
            JSON.parse(event.body)
          );
        }
        break;
    }
    if (response) {
      if (response.errors && response.errors.length) {
        done(null, {
          success: false,
          errors: response.errors,
        });
      } else {
        done(null, {
          success: true,
          content: response,
        });
      }
    } else {
      done(null, {
        success: false,
        errors: [{ code: "GENE-0002" }],
      });
    }
  } else if (event.action) {
    console.log("action: ", event.action);
    switch (event.action) {
      case "create":
        response = await create(event.userId, event.note.claimId, event.note);
        break;
      case "updateProcessed":
        response = await updateProcessed(event.userId, event.noteId, event);
        break;
    }
    if (response) {
      if (response.errors && response.errors.length) {
        console.log(response);
        return {
          success: false,
          errors: response.errors,
        };
      } else {
        return {
          success: true,
          content: response,
        };
      }
    } else {
      return {
        success: false,
        errors: [{ code: "GENE-0002" }],
      };
    }
  }
};

async function getById(claimId, noteId) {
  let errors = [];
  if (!claimId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: claimId" });
  } else if (!claimId.startsWith("clm_")) {
    errors.push({ code: "GENE-0004", message: "Invalid parameter: claimId" });
  } else if (!noteId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: noteId" });
  } else if (!noteId.startsWith("nte_")) {
    errors.push({ code: "GENE-0004", message: "Invalid parameter: noteId" });
  } else {
    let paramsSearch = {
      TableName: "notes",
      KeyConditionExpression: "#pk = :pk and #sk = :sk",
      ExpressionAttributeNames: {
        "#pk": "claimId",
        "#sk": "noteId",
      },
      ExpressionAttributeValues: {
        ":pk": claimId,
        ":sk": noteId,
      },
    };
    try {
      const dynamoCommand = new QueryCommand(paramsSearch);
      const resultDb = await docClient.send(dynamoCommand);
      if (resultDb.Items && resultDb.Items.length > 0) {
        return resultDb.Items[0];
      } else {
        errors.push({
          code: "GENE-0005",
          message: "Element not found: " + noteId,
        });
      }
    } catch (err) {
      errors.push({ code: "GENE-0002", message: err });
    }
  }
  return { errors: errors };
}

async function listByClaim(claimId) {
  let errors = [];
  if (!claimId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: claimId" });
  } else if (!claimId.startsWith("clm_")) {
    errors.push({ code: "GENE-0004", message: "Invalid parameter: claimId" });
  } else {
    let paramsSearch = {
      TableName: "notes",
      IndexName: "claimId-createdAt-index",
      KeyConditionExpression: "#pk = :pk",
      ExpressionAttributeNames: {
        "#pk": "claimId",
      },
      ExpressionAttributeValues: {
        ":pk": claimId,
      },
      ScanIndexForward: false,
    };
    try {
      const dynamoCommand = new QueryCommand(paramsSearch);
      const resultDb = await docClient.send(dynamoCommand);
      if (resultDb.Items) {
        return resultDb.Items;
      } else {
        errors.push({
          code: "GENE-0005",
          message: "Element not found: " + claimId,
        });
      }
    } catch (err) {
      errors.push({ code: "GENE-0002", message: err });
    }
  }
  return { errors: errors };
}

async function create(userId, claimId, note) {
  let errors = [];
  if (note.createdBy && note.createdBy != userId) {
    errors.push({ code: "GENE-0004", message: "Invalid parameter: createdBy" });
  }
  if (!claimId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: claimId" });
  }
  if (!note.templateId && !note.note) {
    errors.push({
      code: "GENE-0001",
      message: "Missing parameter: templateId | note",
    });
  }
  if (errors.length) {
    return { errors: errors };
  }

  if (!note.claimId) {
    note.claimId = claimId;
  }
  if (!note.templateId) {
    note.templateId = "tmpl_other";
  }
  let noteId = "nte_" + uuidv4();
  const now = Math.round(Date.now() / 1000);
  note.noteId = noteId;
  note.createdAt = now;
  note.createdBy = userId;
  if (!note.isExternal) {
    note.isExternal = false;
  }
  if (!note.sentToCms) {
    note.sentToCms = false;
  }
  if (note.templateId !== "tmpl_other") {
    let template = await getTemplateNote(userId, note.templateId, note.claimId);
    if (template.errors) {
      return template;
    }
    if (template.templateId !== note.templateId) {
      return {
        errors: [{ code: "GENE-0002", message: "Error getting the template" }],
      };
    }
    note.note = template.template;
  }

  let claim = await getClaimSummaryById(userId, note.claimId);

  if (claim.source && claim.externalId && claim.externalId.trim()) {
    if (note.templateId === "tmpl_left_messg") {
      await createSQSMessageCmsSyncContactMade(
        claim.source,
        claim.cmsId,
        userId,
        note.claimId,
        claim.externalId,
        noteId,
        note.note,
        now,
        note.sendNoteToCms
      );
    } else if (
      note.templateId === "tmpl_scheduled_appt" ||
      note.templateId === "tmpl_resq_adj" ||
      note.templateId === "tmpl_resq_ins" ||
      note.templateId === "tmpl_resq_weath"
    ) {
      await createSQSMessageCmsSyncUpdateAppointment(
        claim.source,
        claim.cmsId,
        userId,
        note.claimId,
        claim.externalId,
        noteId,
        note.note,
        claim.appointment,
        note.sendNoteToCms
      );
    } else if (note.sendNoteToCms === true) {
      await createSQSMessageCmsSyncAddNote(
        claim.source,
        claim.cmsId,
        userId,
        note.claimId,
        claim.externalId,
        noteId,
        note.note,
        note.sendNoteToCms
      );
    }
  }

  note = await save(note);
  return note;
}

async function update(userId, noteId, note) {
  let errors = [];
  if (note.noteId && note.noteId != noteId) {
    console.log(noteId, " != ", note.noteId);
    errors.push({ code: "GENE-0004", message: "Invalid parameter: noteId" });
  }
  if (!note.claimId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: claimId" });
  }
  if (errors.length) {
    return { errors: errors };
  }

  let saved = await getById(note.claimId, noteId);
  if (saved.errors && saved.errors.length) {
    return saved;
  }

  for (const field in note) {
    if (field !== "createdAt" && field !== "createdBy") {
      saved[field] = note[field];
    }
  }

  note = await save(saved);
  return note;
}

async function save(note) {
  let errors = [];
  let params = {
    TableName: "notes",
    Item: note,
  };
  try {
    const dynamoCommand = new PutCommand(params);
    await docClient.send(dynamoCommand);
    return await getById(note.claimId, note.noteId);
  } catch (err) {
    errors.push({ code: "GENE-0002", message: err });
  }
  return { errors: errors };
}

async function getTemplateNote(userId, templateId, claimId) {
  var params = {
    FunctionName: "note-templates", // the lambda function we are going to invoke
    InvocationType: "RequestResponse",
    LogType: "Tail",
    Payload:
      '{ "action" : "getById", "userId" : "' +
      userId +
      '", "templateId" : "' +
      templateId +
      '", "claimId": "' +
      claimId +
      '" }',
  };

  const lambdaCommand = new InvokeCommand(params);
  let response = await lambdaClient.send(lambdaCommand);

  response = new TextDecoder().decode(response.Payload);
  console.log(response);
  response = JSON.parse(response);
  if (!response.success) {
    return response;
  } else {
    return response.content;
  }
}

async function getAllByUserId(userId, processed, type) {
  let errors = [];
  if (!userId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: userId" });
  } else if (!userId.startsWith("usr_")) {
    errors.push({ code: "GENE-0004", message: "Invalid parameter: userId" });
  } else {
    const now = Math.round(Date.now() / 1000);
    const oneMonth = 30 * 24 * 60 * 60;
    let from = now - 1 * oneMonth;

    let ExpressionAttributeNames = {
      "#pk": "createdBy",
      "#f": "createdAt",
    };
    let ExpressionAttributeValues = {
      ":pk": userId,
      ":f": from,
    };
    let filterExpression = "";

    // processed
    if (processed && (processed === "true" || processed === true)) {
      ExpressionAttributeNames["#p"] = "processed";
      ExpressionAttributeValues[":p"] = true;
      filterExpression = "attribute_exists(#p) and #p = :p";
    } else if (processed === "false" || processed === false) {
      ExpressionAttributeNames["#p"] = "processed";
      ExpressionAttributeValues[":p"] = true;
      filterExpression = "#p <> :p";
    }

    // type
    if (type && type === "tmpl_other") {
      ExpressionAttributeNames["#t"] = "templateId";
      ExpressionAttributeValues[":t"] = type;
      if (filterExpression !== "") {
        filterExpression = filterExpression + " and #t = :t";
      } else {
        filterExpression = "#t = :t";
      }
    } else if (type && type === "!tmpl_other") {
      ExpressionAttributeNames["#t"] = "templateId";
      ExpressionAttributeValues[":t"] = "tmpl_other";
      if (filterExpression !== "") {
        filterExpression = filterExpression + " and #t <> :t";
      } else {
        filterExpression = "#t <> :t";
      }
    }

    let paramsSearch = {
      TableName: "notes",
      IndexName: "createdBy-index",
      KeyConditionExpression: "#pk = :pk and #f > :f",
      ExpressionAttributeNames: ExpressionAttributeNames,
      ExpressionAttributeValues: ExpressionAttributeValues,
      ScanIndexForward: false,
    };

    //
    if (filterExpression !== "") {
      paramsSearch = {
        TableName: "notes",
        IndexName: "createdBy-index",
        KeyConditionExpression: "#pk = :pk and #f > :f",
        FilterExpression: filterExpression,
        ExpressionAttributeNames: ExpressionAttributeNames,
        ExpressionAttributeValues: ExpressionAttributeValues,
        ScanIndexForward: false,
      };
    }

    // query
    try {
      const dynamoCommand = new QueryCommand(paramsSearch);
      const resultDb = await docClient.send(dynamoCommand);

      let result = [];
      let claims = {};
      for (let item of resultDb.Items) {
        // note
        let newItem = {
          noteId: item.noteId,
          note: item.note,
          processed: item.processed ? item.processed : false,
          createdAt: item.createdAt,
          claimId: item.claimId,
        };
        if (!claims[item.claimId]) {
          // claim
          let claim = await getClaimSummaryById(userId, item.claimId);
          if (claim.errors) {
            continue;
          }

          newItem.insuredFullName = claim.insuredFullName;
          newItem.claimNo = claim.claimNo;
          newItem.statusClaim = claim.status;
          newItem.claimSource = claim.source;
          newItem.claimExternalId = claim.externalId;
          newItem.claimFirmCarrier = claim.firmCarrier;

          claims[item.claimId] = {
            claimId: item.claimId,
            insuredFullName: claim.insuredFullName,
            claimNo: claim.claimNo,
            statusClaim: claim.status,
            claimSource: claim.source,
            claimExternalId: claim.externalId,
            claimFirmCarrier: claim.firmCarrier,
          };
        } else {
          const pickedClaim = claims[item.claimId];
          newItem.insuredFullName = pickedClaim.insuredFullName;
          newItem.claimNo = pickedClaim.claimNo;
          newItem.statusClaim = pickedClaim.statusClaim;
          newItem.claimSource = pickedClaim.claimSource;
          newItem.claimExternalId = pickedClaim.claimExternalId;
          newItem.claimFirmCarrier = pickedClaim.claimFirmCarrier;
        }

        // push
        if (newItem.statusClaim !== "Cancelled") {
          result.push(newItem);
        }
      }
      return result;
    } catch (err) {
      errors.push({ code: "GENE-0002", message: err });
    }
  }
  return { errors: errors };
}

async function getClaimSummaryById(inspectorId, claimId) {
  return await getClaimById(inspectorId, claimId);

  let errors = [];
  if (!claimId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: claimId" });
  } else if (!claimId.startsWith("clm_")) {
    errors.push({
      code: "GENE-0004",
      message: "Invalid parameter: claimId: " + claimId,
    });
  } else if (!inspectorId) {
    errors.push({
      code: "GENE-0001",
      message: "Missing parameter: inspectorId",
    });
  } else if (!inspectorId.startsWith("usr_")) {
    errors.push({
      code: "GENE-0004",
      message: "Invalid parameter: inspectorId: " + inspectorId,
    });
  } else {
    let paramsSearch = {
      TableName: "claims",
      KeyConditionExpression: "#pk = :pk and begins_with(#sk, :sk)",
      ExpressionAttributeNames: {
        "#pk": "pk",
        "#sk": "sk",
        "#s": "status",
        "#so": "source",
      },
      ExpressionAttributeValues: {
        ":pk": inspectorId + "/" + claimId,
        ":sk": "info_",
      },
      ProjectionExpression:
        "inspectorId, claimId, lossType, insuredFullName, claimNo, createdAt, firmCarrier, lossTypeOther, #s, #so, externalId, appointment, source, externalId",
    };
    try {
      const dynamoCommand = new QueryCommand(paramsSearch);
      const resultDb = await docClient.send(dynamoCommand);
      if (resultDb.Items && resultDb.Items.length > 0) {
        return resultDb.Items[0];
      } else {
        errors.push({
          code: "GENE-0005",
          message: "Element not found: " + claimId,
        });
      }
    } catch (err) {
      errors.push({ code: "GENE-0002", message: err });
    }
  }
  return { errors: errors };
}

async function getClaimById(userId, claimId) {
  console.log("getClaimById userId: " + userId + ", claimId: " + claimId);
  var params = {
    FunctionName: "claims", // the lambda function we are going to invoke
    InvocationType: "RequestResponse",
    LogType: "Tail",
    Payload:
      '{ "action" : "getById", "inspectorId" : "' +
      userId +
      '", "claimId": "' +
      claimId +
      '" }',
  };

  const lambdaCommand = new InvokeCommand(params);
  let response = await lambdaClient.send(lambdaCommand);

  response = new TextDecoder().decode(response.Payload);
  console.log(response);
  response = JSON.parse(response);
  if (!response.success) {
    return {
      errors: [{ code: "GENE-0002", message: "Error getting the claim by id" }],
    };
  } else {
    return response.content;
  }
}

async function updateProcessed(userId, noteId, request) {
  let errors = [];
  if (!userId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: userId" });
  } else if (!noteId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: noteId" });
  } else if (request && !request.claimId) {
    errors.push({ code: "GENE-0001", message: "Missing parameter: claimId" });
  }
  if (errors.length) {
    return { errors: errors };
  }

  let params = {
    TableName: "notes",
    Key: {
      claimId: request.claimId,
      noteId: noteId,
    },
    UpdateExpression: "set #p = :p",
    ExpressionAttributeValues: {
      ":p": request.status,
    },
    ExpressionAttributeNames: {
      "#p": "processed",
    },
    ReturnValues: "UPDATED_NEW",
  };
  const now = Math.round(Date.now() / 1000);
  if (request.sentToCms === true) {
    params.UpdateExpression += ", #stc = :stc, #stca = :stca";
    params.ExpressionAttributeNames["#stc"] = "sentToCms";
    params.ExpressionAttributeNames["#stca"] = "sentToCmsAt";
    params.ExpressionAttributeValues[":stc"] = request.sentToCms;
    params.ExpressionAttributeValues[":stca"] = now;
  }

  try {
    const dynamoCommand = new UpdateCommand(params);
    const resultDb = await docClient.send(dynamoCommand);
    if (resultDb && resultDb.Attributes) {
      return "done";
    } else {
      console.log("No answer from updateProcessed query.");
      errors.push({ code: "GENE-0002", message: "Internal Error" });
    }
  } catch (err) {
    console.log("err:", err);
    errors.push({ code: "GENE-0002", message: err });
  }

  return { errors: errors };
}

async function createSQSMessageCmsSyncContactMade(
  system,
  cmsId,
  inspectorId,
  claimId,
  claimExternalId,
  noteId,
  note,
  timestamp,
  sendNoteToCms
) {
  try {
    const message = {
      action: "markContactMade",
      system,
      cmsId,
      inspectorId,
      claimId,
      claimExternalId,
      noteId,
      note,
      timestamp,
      sendNoteToCms,
    };
    console.log("createSQSMessageCmsSyncContactMade: ", message);

    const sqsCommand = new SendMessageCommand({
      MessageBody: JSON.stringify(message),
      QueueUrl: process.env.SQS_URL_CmsSyncBase + system + "_contact_made.fifo",
      MessageGroupId: inspectorId + ":" + system + ":" + claimExternalId,
    });
    const response = await sqsClient.send(sqsCommand);

    console.log(
      "Message put on queue " +
        process.env.SQS_URL_CmsSyncBase +
        system +
        "_contact_made.fifo"
    );
  } catch (e) {
    console.log(
      "Exception on queue " +
        process.env.SQS_URL_CmsSyncBase +
        system +
        "_contact_made.fifo",
      e
    );
  }
}

async function createSQSMessageCmsSyncUpdateAppointment(
  system,
  cmsId,
  inspectorId,
  claimId,
  claimExternalId,
  noteId,
  note,
  timestamp,
  sendNoteToCms
) {
  try {
    const message = {
      action: "updateAppointment",
      system,
      cmsId,
      inspectorId,
      claimId,
      claimExternalId,
      noteId,
      note,
      timestamp,
      sendNoteToCms,
    };

    console.log("createSQSMessageCmsSyncUpdateAppointment: ", message);

    const sqsCommand = new SendMessageCommand({
      MessageBody: JSON.stringify(message),
      QueueUrl:
        process.env.SQS_URL_CmsSyncBase + system + "_update_appointment.fifo",
      MessageGroupId: inspectorId + ":" + system + ":" + claimExternalId,
    });
    const response = await sqsClient.send(sqsCommand);

    console.log(
      "Message put on queue " +
        process.env.SQS_URL_CmsSyncBase +
        system +
        "_update_appointment.fifo"
    );
  } catch (e) {
    console.log(
      "Exception on queue " +
        process.env.SQS_URL_CmsSyncBase +
        system +
        "_update_appointment.fifo",
      e
    );
  }
}

async function createSQSMessageCmsSyncAddNote(
  system,
  cmsId,
  inspectorId,
  claimId,
  claimExternalId,
  noteId,
  note,
  sendNoteToCms
) {
  try {
    const message = {
      action: "addNote",
      system,
      cmsId,
      inspectorId,
      claimId,
      claimExternalId,
      noteId,
      note,
      sendNoteToCms,
    };

    console.log("createSQSMessageCmsSyncAddNote: ", message);

    const sqsCommand = new SendMessageCommand({
      MessageBody: JSON.stringify(message),
      QueueUrl: process.env.SQS_URL_CmsSyncBase + system + "_add_note.fifo",
      MessageGroupId: inspectorId + ":" + system + ":" + claimExternalId,
    });
    const response = await sqsClient.send(sqsCommand);

    console.log(
      "Message put on queue " +
        process.env.SQS_URL_CmsSyncBase +
        system +
        "_add_note.fifo"
    );
  } catch (e) {
    console.log(
      "Exception on queue " +
        process.env.SQS_URL_CmsSyncBase +
        system +
        "_add_note.fifo",
      e
    );
  }
}
