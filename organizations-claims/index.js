import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { Connection } from "postgresql-client";
import {
  CognitoIdentityProviderClient,
  AdminUpdateUserAttributesCommand,
  AdminCreateUserCommand,
  AdminAddUserToGroupCommand,
  ChangePasswordCommand,
  NotAuthorizedException,
  InvalidPasswordException,
  LimitExceededException,
} from "@aws-sdk/client-cognito-identity-provider";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { v4 as uuidv4 } from "uuid";
//var googlemaps = require('@googlemaps/google-maps-services-js');
import { Client } from "@googlemaps/google-maps-services-js";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  TransactWriteCommand,
  GetCommand,
  QueryCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
const googlemapsClient = new Client({});

const UserPoolId = "us-east-1_gJKpA0Wix";
const cognitoClient = new CognitoIdentityProviderClient();
const lambdaClient = new LambdaClient();
const sqsClient = new SQSClient();

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
    console.log(event.requestContext.authorizer.claims);
    const userId =
      event.requestContext.authorizer.claims["custom:user_id"].slice(4);
    const isAdmin =
      event.requestContext.authorizer.claims["cognito:groups"] &&
      event.requestContext.authorizer.claims["cognito:groups"].includes(
        "Admin"
      );
    const userFullName = event.requestContext.authorizer.claims.name;
    const userEmail = event.requestContext.authorizer.claims.email;
    console.log("userId: ", userId + " " + isAdmin);
    try {
      switch (event.resource) {
        case "/organizations/{organizationId}/claims/unassigned":
          if (event.httpMethod === "GET") {
            response = await getUnassignedClaimsByOrg(
              userId,
              event.pathParameters.organizationId,
              event.queryStringParameters?.polygon
            );
          }
          if (event.httpMethod === "DELETE") {
            response = await deleteUnassignedClaims(
              userId,
              event.pathParameters.organizationId,
              JSON.parse(event.body)
            );
          }
          break;
        case "/organizations/{organizationId}/claims/unassigned/{claimId}":
          if (event.httpMethod === "PUT") {
            response = await updateUnassignedClaim(
              userId,
              event.pathParameters.organizationId,
              JSON.parse(event.body)
            );
          }
          break;
        case "/organizations/{organizationId}/claims/open":
          if (event.httpMethod === "GET") {
            response = await getOpenClaimsByOrg(
              userId,
              event.pathParameters.organizationId,
              event.queryStringParameters?.polygon
            );
          }
          break;
        case "/organizations/{organizationId}/claims/inspected":
          if (event.httpMethod === "GET") {
            response = await getInspectedClaimsByOrg(
              userId,
              event.pathParameters.organizationId,
              event.queryStringParameters?.polygon
            );
          }
          break;
        case "/organizations/{organizationId}/claims":
          if (event.httpMethod === "POST") {
            response = await createClaimByOrg(
              userId,
              event.pathParameters.organizationId,
              JSON.parse(event.body)
            );
          }
          break;
        case "/organizations/{organizationId}/claims/assign":
          if (event.httpMethod === "POST") {
            response = await assignOrgClaimsToUser(
              userId,
              event.pathParameters.organizationId,
              JSON.parse(event.body)
            );
          }
          break;
        case "/organizations/{organizationId}/claims/{claimId}/flow":
          if (event.httpMethod === "POST") {
            let request = JSON.parse(event.body);
            response = await updateFlow(
              userId,
              event.pathParameters.organizationId,
              event.pathParameters.claimId,
              request
            );
          }
          break;
        case "/calendar/{userId}/unavailable":
          if (event.httpMethod === "GET") {
            response = await getUnavailableEvents(
              userId,
              event.pathParameters.userId
            );
          }
          break;
        case "/organizations/{organizationId}/calendar/{userId}/unavailable":
          if (event.httpMethod === "GET") {
            response = await getUnavailableEvents(
              userId,
              event.pathParameters.userId,
              event.pathParameters.organizationId
            );
          }
          break;
        case "/organizations/{organizationId}/claims/{claimId}/messages":
          if (event.httpMethod === "GET") {
            response = await getClaimMessages(
              userId,
              event.pathParameters.organizationId,
              event.pathParameters.claimId
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
  } else if (event.action) {
    console.log(event);
    try {
      switch (event.action) {
        case "switchAllOpenAjusterClaimsToOrg":
          response = await switchAllOpenAjusterClaimsToOrg(
            event.organizationId
          );
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

async function getUnassignedClaimsByOrg(userId, organizationId, polygon) {
  if (!userId) {
    throw new GpException(400, "Field `userId` is required");
  }
  if (!organizationId) {
    throw new GpException(400, "Field `organizationId` is required");
  }
  if (organizationId.startsWith("org_")) {
    organizationId = organizationId.slice(4);
  }

  let success = false;
  let claims = [];
  const connection = new Connection(connectionString);
  try {
    await connection.connect();

    let query =
      "select * from claims where organization_id = $1 and inspector_id IS NULL order by created_at desc";
    let params = [organizationId];

    if (polygon) {
      polygon = JSON.parse(polygon);
      if (polygon.length > 0) {
        let polygonStr = "POLYGON(( ";
        let prefix = "";
        for (let point of polygon) {
          polygonStr += prefix + point[1] + " " + point[0];
          prefix = ", ";
        }

        // If the polygon is't closed, at the first point at the end to close it
        if (
          polygon[0][0] !== polygon[polygon.length - 1][0] ||
          polygon[0][1] !== polygon[polygon.length - 1][1]
        ) {
          polygonStr += prefix + polygon[0][1] + " " + polygon[0][0];
        }

        polygonStr += " ))";
        console.log(polygonStr);

        query =
          "select * from claims where organization_id = $1 and inspector_id IS NULL and ST_Intersects(ST_GeographyFromText($2)::geography, coordinates)  order by created_at desc";
        params.push(polygonStr);
      }
    }
    const result = await connection.query(query, { fetchCount: 10000, params });

    success = true;

    for (let claim of result.rows) {
      claims.push(_convertClaim(result.fields, claim));
    }
  } catch (error) {
    console.log("Error getUnassignedClaimsByOrg: ", error);
    throw new GpException(500, "Server Error: " + error);
  } finally {
    await connection.close();
  }

  return claims;
}

async function deleteUnassignedClaims(userId, organizationId, claimIds) {
  console.log(
    "deleteUnassignedClaims: ",
    "userId: " +
      userId +
      ", organizationId: " +
      organizationId +
      ", claimIds: " +
      claimIds
  );
  if (!userId) {
    throw new GpException(400, "Field `userId` is required");
  }
  if (!organizationId) {
    throw new GpException(400, "Field `organizationId` is required");
  }
  if (!claimIds || claimIds.length === 0) {
    throw new GpException(400, "Field `claimIds` is required");
  }
  if (organizationId.startsWith("org_")) {
    organizationId = organizationId.slice(4);
  }

  let success = false;
  const connection = new Connection(connectionString);
  await connection.connect();
  try {
    await connection.query("BEGIN");
    for (let claimId of claimIds) {
      if (claimId.startsWith("clm_")) {
        claimId = claimId.slice(4);
      }
      const result = await connection.query(
        "DELETE FROM claims WHERE id = $1 AND inspector_id IS NULL AND status = $2 AND organization_id = $3",
        { params: [claimId, "New Claim", organizationId] }
      );
      console.log(result);
    }

    await connection.query("COMMIT");
    success = true;
  } catch (err) {
    console.log(err);
    await connection.query("ROLLBACK");
  } finally {
    await connection.close();
  }
  if (success) {
    return "done";
  } else {
    throw new GpException(500, "Database Error");
  }
}

async function getOpenClaimsByOrg(userId, organizationId) {
  if (!userId) {
    throw new GpException(400, "Field `userId` is required");
  }
  if (!organizationId) {
    throw new GpException(400, "Field `organizationId` is required");
  } else if (!organizationId.startsWith("org_")) {
    organizationId = "org_" + organizationId;
  }

  const claims = await _callLambdaFunction("claims", {
    action: "getOpenClaimsByOrg",
    organizationId,
  });
  if (claims === undefined) {
    throw new GpException(500, "Error getOpenClaimsByOrg");
  }
  return claims;
}

async function getInspectedClaimsByOrg(userId, organizationId) {
  if (!userId) {
    throw new GpException(400, "Field `userId` is required");
  }
  if (!organizationId) {
    throw new GpException(400, "Field `organizationId` is required");
  } else if (!organizationId.startsWith("org_")) {
    organizationId = "org_" + organizationId;
  }

  const claims = await _callLambdaFunction("claims", {
    action: "getInspectedClaimsByOrg",
    organizationId,
  });
  if (claims === undefined) {
    throw new GpException(500, "Error getInspectedClaimsByOrg");
  }
  return claims;
}

async function updateFlow(userId, organizationId, claimId, request) {
  if (!userId) {
    throw new GpException(400, "Field `userId` is required");
  }
  if (!organizationId) {
    throw new GpException(400, "Field `organizationId` is required");
  } else if (!organizationId.startsWith("org_")) {
    organizationId = "org_" + organizationId;
  }
  if (request.claimId !== claimId) {
    throw new GpException(400, "Invalid field `claimId`");
  }
  if (request.organizationId !== organizationId) {
    throw new GpException(400, "Invalid field `organizationId`");
  }

  let requestLambda = {
    claim: request,
    action: "updateFlow",
    claimId: request.claimId,
    userId: request.inspectorId,
    newInspectorId: request.newInspectorId,
  };
  console.log(requestLambda);
  const flowResponse = await _callLambdaFunction("claims", requestLambda, true);
  if (flowResponse === undefined) {
    throw new GpException(500, "Error updateFlow");
  }
  return flowResponse.success
    ? flowResponse.content
    : { errors: flowResponse.errors };
}

async function createClaimByOrg(userId, organizationId, claim) {
  if (claim.createdBy && claim.createdBy != userId) {
    throw new GpException(400, "Invalid `createdBy` value.");
  }
  if (!claim.location) {
    throw new GpException(400, "Field `location` is required");
  }

  if (userId.startsWith("usr_")) {
    userId = userId.slice(4);
  }
  if (organizationId.startsWith("org_")) {
    organizationId = organizationId.slice(4);
  }
  let claimId = "clm_" + uuidv4();
  const now = Math.round(Date.now() / 1000);
  claim.claimId = claimId;
  claim.createdAt = now;
  claim.createdBy = userId;
  claim.organizationId = organizationId;
  claim.status = "New Claim";

  if (claim.location) {
    if (claim.location.zipcode === "undefined") {
      claim.location.zipcode = undefined;
    }
    let location = await _getLocation(claim.location);
    if (location && !location.errors && location.lat && location.lng) {
      claim.location.coordinates = [location.lat, location.lng];
      if (!claim.location.country && location.country) {
        claim.location.country = location.country;
      }
    }
  }

  if (claim.phone1 && claim.phone1.trim() !== "") {
    let phoneDetails = await _twilioPhoneDetails(claim.phone1.trim());
    claim.phone1Details = phoneDetails;
  }
  if (claim.phone2 && claim.phone2.trim() !== "") {
    let phoneDetails = await _twilioPhoneDetails(claim.phone2.trim());
    claim.phone2Details = phoneDetails;
  }
  if (claim.phone3 && claim.phone3.trim() !== "") {
    let phoneDetails = await _twilioPhoneDetails(claim.phone3.trim());
    claim.phone3Details = phoneDetails;
  }

  claim = await _createClaimDB(claim);

  return claim;
}

async function updateUnassignedClaim(userId, organizationId, claim) {
  if (!claim.claimId) {
    throw new GpException(400, "Field `claimId` is required");
  }
  if (!claim.location) {
    throw new GpException(400, "Field `location` is required");
  }

  if (userId.startsWith("usr_")) {
    userId = userId.slice(4);
  }
  if (organizationId.startsWith("org_")) {
    organizationId = organizationId.slice(4);
  }

  if (claim.location) {
    if (claim.location.zipcode === "undefined") {
      claim.location.zipcode = undefined;
    }
    let location = await _getLocation(claim.location);
    if (location && !location.errors && location.lat && location.lng) {
      claim.location.coordinates = [location.lat, location.lng];
      if (!claim.location.country && location.country) {
        claim.location.country = location.country;
      }
    }
  }

  if (claim.phone1 && claim.phone1.trim() !== "") {
    let phoneDetails = await _twilioPhoneDetails(claim.phone1.trim());
    claim.phone1Details = phoneDetails;
  }
  if (claim.phone2 && claim.phone2.trim() !== "") {
    let phoneDetails = await _twilioPhoneDetails(claim.phone2.trim());
    claim.phone2Details = phoneDetails;
  }
  if (claim.phone3 && claim.phone3.trim() !== "") {
    let phoneDetails = await _twilioPhoneDetails(claim.phone3.trim());
    claim.phone3Details = phoneDetails;
  }

  claim = await _updateClaimDB(claim);

  return claim;
}

async function assignOrgClaimsToUser(userId, organizationId, event) {
  if (!event.claimIds || event.claimIds.length === 0) {
    throw new GpException(400, "Invalid `claimIds` value.");
  }
  if (!event.inspectorId) {
    throw new GpException(400, "Field `inspectorId` is required");
  }

  // Validate the logged user is Admin in the Organization
  const loggedUser = await _getUserById(userId);
  const organizations = await _getOrganizationsByUserId(userId);
  let isOrgAdmin = false;
  for (const org of organizations) {
    if (org.organizationId === organizationId && org.roles.includes("Admin")) {
      isOrgAdmin = true;
    }
  }
  if (!isOrgAdmin) {
    throw new GpException(
      403,
      "Access denied, only Organization Admins can do this action."
    );
  }

  // Validate the Inspector is Field Agent in the Organization
  const organizationsInspector = await _getOrganizationsByUserId(
    event.inspectorId
  );
  let isOrgFieldAgent = false;
  for (const org of organizationsInspector) {
    if (
      org.organizationId === organizationId &&
      org.roles.includes("Field_Agent")
    ) {
      isOrgFieldAgent = true;
    }
  }
  if (!isOrgFieldAgent) {
    throw new GpException(
      400,
      "Invalid Role of inspector in the organization."
    );
  }

  const claims = [];
  for (let claimId of event.claimIds) {
    const claim = await _getClaimByIdAndOrganizationDB(organizationId, claimId);
    if (!claim) {
      throw new GpException(404, "Not found claim: " + claimId);
    }
    if (claim.inspectorId || claim.status !== "New Claim") {
      throw new GpException(422, "Invalid status of the claim: " + claimId);
    }

    claim.assignedBy = userId;
    claim.inspectorId = event.inspectorId;

    claims.push(claim);
  }

  let error = false;
  for (let claim of claims) {
    try {
      await _assignOrgClaimToUser(claim, event.inspectorId);
    } catch (err) {
      console.log(
        "Error assignOrgClaimsToUser " + claim.claimId + " " + event.inspectorId
      );
      console.log(err);
      error = true;
    }
  }

  if (error) {
    throw new GpException(500, "Not all claims were assigned, server error.");
  }

  return "done";
}

async function getUnavailableEvents(loggedUserId, inspectorId, organizationId) {
  if (!loggedUserId) {
    throw new GpException(400, "Field `loggedUserId` is required.");
  }
  if (!inspectorId) {
    throw new GpException(400, "Field `inspectorId` is required.");
  } else if (!inspectorId.startsWith("usr_")) {
    inspectorId = "usr_" + inspectorId;
  }

  let result = [];
  let userEvents = await _callLambdaFunction("calendar-events", {
    inspectorId,
    action: "getCalendarEvents",
  });
  if (userEvents === undefined) {
    throw new GpException(500, "Error calendar-events");
  } else {
    userEvents = userEvents
      .filter((event) => event.unavailable)
      .map((event) => {
        delete event.description;
        return event;
      });
    for (let event of userEvents) {
      result.push(event);
    }
  }

  if (organizationId) {
    const claims = await _callLambdaFunction("claims", {
      action: "getOpenClaimsOutOfOrg",
      organizationId,
      inspectorId,
    });
    if (claims === undefined) {
      throw new GpException(500, "Error getOpenClaimsOutOfOrg");
    } else {
      for (const claim of claims) {
        result.push(claim);
      }
    }
  }

  return result;
}
async function getClaimMessages(loggedUserId, organizationId, claimId) {
  if (!loggedUserId) {
    throw new GpException(400, "Field `loggedUserId` is required.");
  }
  if (!organizationId) {
    throw new GpException(400, "Field `organizationId` is required.");
  }
  if (!claimId) {
    throw new GpException(400, "Field `claimId` is required.");
  }

  const messages = await _callLambdaFunction("messenger", {
    action: "getClaimMessagesByIdAndOrg",
    organizationId,
    claimId,
  });
  if (messages === undefined) {
    throw new GpException(500, "Error getClaimMessagesByIdAndOrg");
  }
  return messages;
}

async function _assignOrgClaimToUser(claim, inspectorId) {
  if (claim.claimId.startsWith("clm_")) {
    claim.claimId = claim.claimId.slice(4);
  }
  if (inspectorId.startsWith("usr_")) {
    inspectorId = inspectorId.slice(4);
  }

  let success = false;
  const connection = new Connection(connectionString);
  await connection.connect();
  try {
    const now = Math.round(Date.now() / 1000);
    await connection.query("BEGIN");
    const result = await connection.query(
      "UPDATE claims SET inspector_id = $2, updated_at = $3 WHERE id = $1 AND inspector_id IS NULL AND status = $4",
      { params: [claim.claimId, inspectorId, now, "New Claim"] }
    );
    console.log(result);

    const claimDynamo = await _callLambdaFunction("claims", {
      ...claim,
      action: "createClaimByOrg",
    });
    if (!claimDynamo || !claimDynamo.claimId) {
      throw new GpException(500, "_callLambdaFunction createClaimByOrg Error");
    }

    await connection.query("COMMIT");
    success = true;
  } catch (err) {
    console.log(err);
    await connection.query("ROLLBACK");
  } finally {
    await connection.close();
  }
  if (success) {
    return claim;
  } else {
    throw new GpException(500, "Database Error");
  }
}

async function switchAllOpenAjusterClaimsToOrg(organizationId) {
  if (!organizationId) {
    throw new GpException(400, "Field `organizationId` is required");
  } /*else if(!organizationId.startsWith('org_')) {
        organizationId = 'org_' + organizationId;
    }
    */
  const users = await _listUserIdsByOrganization(organizationId);
  let count = 0;
  for (const row of users) {
    const userId = "usr_" + row[0];
    const userClaims = await _callLambdaFunction("claims", {
      action: "getOpenClaimsByInspectorId",
      inspectorId: userId,
      idsOnly: true,
    });
    if (userClaims === undefined) {
      throw new GpException(500, "Error getOpenClaimsByOrg");
    }
    for (const claim of userClaims) {
      if (claim.sk.startsWith("info_") && claim.claimId) {
        await _setOrganizationToClaim(claim, "org_" + organizationId);
        count++;
      }
    }
  }
  return count;
}

async function _setOrganizationToClaim(claim, organizationId) {
  console.log("_setOrganizationToClaim");
  /*if(!organizationId) {
        throw new GpException(400, 'Field `organizationId` is required');
    }*/
  if (!claim.pk) {
    throw new GpException(400, "Field `claim.pk` is required");
  }
  if (!claim.sk) {
    throw new GpException(400, "Field `claim.sk` is required");
  }
  let input = {
    TransactItems: [
      {
        Update: {
          TableName: "claims",
          Key: {
            pk: claim.pk,
            sk: claim.sk,
          },
          UpdateExpression: "set #org = :org",
          ExpressionAttributeNames: {
            "#org": "organizationId",
          },
          ExpressionAttributeValues: {
            ":org": organizationId,
          },
        },
      },
    ],
  };
  if (!organizationId) {
    input = {
      TransactItems: [
        {
          Update: {
            TableName: "claims",
            Key: {
              pk: claim.pk,
              sk: claim.sk,
            },
            UpdateExpression: "REMOVE #org",
            ExpressionAttributeNames: {
              "#org": "organizationId",
            },
          },
        },
      ],
    };
  }
  try {
    const command = new TransactWriteCommand(input);
    const response = await docClient.send(command);
    console.log(response);
  } catch (err) {
    console.log(err);
    throw new GpException(500, err);
  }
}

async function _listUserIdsByOrganization(organizationId) {
  console.log("_listUserIdsByOrganization: ", organizationId);
  const connection = new Connection(connectionString);
  let users = [];
  let success = false;
  let result;
  try {
    await connection.connect();
    result = await connection.query(
      "select uo.user_id from user_organization as uo where uo.organization_id = $1  and 'Field_Agent'=ANY(uo.roles)",
      { params: [organizationId], fetchCount: 10000 }
    );
    success = true;
  } catch (err) {
    console.log(err);
  } finally {
    await connection.close();
  }

  if (success) {
    console.log("_listUserIdsByOrganization: ", result.rows.length);
    return result.rows;
  }
}

async function _getUserById(userId) {
  const user = await _callLambdaFunction("accounts-users", {
    action: "getUserById",
    userId,
  });
  if (!user) {
    console.log("Not found user: " + userId);
    throw new GpException(404, "Not found user: " + userId);
  }
  return user;
}

async function _getOrganizationsByUserId(userId) {
  return await _callLambdaFunction("accounts-users", {
    action: "getOrganizationsByUserId",
    userId,
  });
}

async function _getClaimByIdAndOrganizationDB(organizationId, claimId) {
  let success = false;
  let claim = undefined;

  if (claimId.startsWith("clm_")) {
    claimId = claimId.slice(4);
  }
  if (organizationId.startsWith("org_")) {
    organizationId = organizationId.slice(4);
  }

  const connection = new Connection(connectionString);
  try {
    await connection.connect();

    let query = "select * from claims where organization_id = $1 and id = $2";
    let params = [organizationId, claimId];

    const result = await connection.query(query, { params });

    success = true;

    if (result.rows.length > 0) {
      claim = _convertClaim(result.fields, result.rows[0]);
    }
  } catch (error) {
    console.log("Error _getClaimByIdAndOrganizationDB: ", error);
    throw new GpException(500, "Server Error: " + error);
  } finally {
    await connection.close();
  }

  return claim;
}

async function _createClaimDB(claim) {
  let success = false;
  const connection = new Connection(connectionString);
  await connection.connect();
  try {
    await connection.query("BEGIN");

    const lat =
      claim.location?.coordinates && claim.location.coordinates.length === 2
        ? claim.location.coordinates[0]
        : null;
    const lng =
      claim.location?.coordinates && claim.location.coordinates.length === 2
        ? claim.location.coordinates[1]
        : null;
    const point = lat && lng ? `POINT(${lng} ${lat})` : null;
    const result = await connection.query(
      "INSERT INTO claims(organization_id, claim_no, firm_carrier, inspector_id, insured_full_name, policyholder, location, loss_type, loss_type_other, created_at, created_by, email, phone1, phone2, phone3, ext1, ext2, ext3, phone1_details, phone2_details, phone3_details, source, external_id, status, coordinates) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, ST_GeographyFromText($25) );",
      {
        params: [
          claim.organizationId,
          claim.claimNo,
          claim.firmCarrier,
          claim.inspectorId,
          claim.insuredFullName,
          claim.policyholder,
          claim.location,
          claim.lossType,
          claim.lossTypeOther,
          claim.createdAt,
          claim.createdBy,
          claim.email,
          claim.phone1,
          claim.phone2,
          claim.phone3,
          claim.ext1,
          claim.ext2,
          claim.ext3,
          claim.phone1Details,
          claim.phone2Details,
          claim.phone3Details,
          claim.source,
          claim.externalId,
          claim.status,
          point,
        ],
      }
    );
    console.log(result);
    await connection.query("COMMIT");
    success = true;
  } catch (err) {
    console.log(err);
    await connection.query("ROLLBACK");
  } finally {
    await connection.close();
  }
  if (success) {
    return claim;
  } else {
    throw new GpException(500, "Database Error");
  }
}

async function _updateClaimDB(claim) {
  let success = false;

  if (claim.claimId.startsWith("clm_")) {
    claim.claimId = claim.claimId.slice(4);
  }

  const connection = new Connection(connectionString);
  await connection.connect();
  try {
    await connection.query("BEGIN");

    const lat =
      claim.location?.coordinates && claim.location.coordinates.length === 2
        ? claim.location.coordinates[0]
        : null;
    const lng =
      claim.location?.coordinates && claim.location.coordinates.length === 2
        ? claim.location.coordinates[1]
        : null;
    const point = lat && lng ? `POINT(${lng} ${lat})` : null;
    const result = await connection.query(
      "UPDATE claims SET claim_no = $2, firm_carrier = $3, insured_full_name = $4, policyholder = $5, location = $6, loss_type = $7, loss_type_other = $8, email = $9, phone1 = $10, phone2 = $11, phone3 = $12, ext1 = $13, ext2 = $14, ext3 = $15, phone1_details = $16, phone2_details = $17, phone3_details = $18,coordinates = ST_GeographyFromText($19) WHERE id = $1;",
      {
        params: [
          claim.claimId,
          claim.claimNo,
          claim.firmCarrier,
          claim.insuredFullName,
          claim.policyholder,
          claim.location,
          claim.lossType,
          claim.lossTypeOther,
          claim.email,
          claim.phone1,
          claim.phone2,
          claim.phone3,
          claim.ext1,
          claim.ext2,
          claim.ext3,
          claim.phone1Details,
          claim.phone2Details,
          claim.phone3Details,
          point,
        ],
      }
    );
    console.log(result);
    await connection.query("COMMIT");
    success = true;
  } catch (err) {
    console.log(err);
    await connection.query("ROLLBACK");
  } finally {
    await connection.close();
  }
  if (success) {
    return claim;
  } else {
    throw new GpException(500, "Database Error");
  }
}

async function _getLocation(location) {
  let address = "";
  if (location.address1 && location.address1.trim().length > 0) {
    address = location.address1.trim();
  }
  if (location.address2 && location.address2.trim().length > 0) {
    address += "," + location.address2.trim();
  }
  if (location.city && location.city.trim().length > 0) {
    address += "," + location.city.trim();
  }
  if (location.stateCode && location.stateCode.trim().length > 0) {
    address += "," + location.stateCode.trim();
  } else if (location.stateName && location.stateName.trim().length > 0) {
    address += "," + location.stateName.trim();
  }
  if (location.zipcode && location.zipcode.trim().length > 0) {
    address += "," + location.zipcode.trim();
  }
  if (!address || address.length === 0) {
    return null;
  }
  try {
    let res = await googlemapsClient.geocode({
      params: {
        address: address,
        key: process.env.GOOGLE_MAPS_API_KEY,
      },
      timeout: 1000, // milliseconds
    });

    if (res && res.data && res.data.results.length) {
      let loc = res.data.results[0].geometry.location;
      for (let component of res.data.results[0].address_components) {
        if (
          component.short_name &&
          component.types &&
          component.types.length > 0 &&
          component.types[0] === "country"
        ) {
          loc.country = component.short_name.toLowerCase();
          if (loc.country === "pr") {
            loc.country = "us";
          }
        }
      }
      console.log(loc);
      return loc;
    } else {
      return null;
    }
  } catch (err) {
    console.log("Error _getLocation: ", err);
    throw new GpException(500, "Server Error: " + err);
  }
}

function _convertClaim(fields, row) {
  let claim = {};
  let i = 0;
  for (const field of fields) {
    if (field.fieldName === "id") {
      claim["claimId"] = "clm_" + row[i];
    } else {
      let fieldName = _renameField(field.fieldName);
      claim[fieldName] = row[i] == null ? undefined : row[i];
    }

    i++;
  }
  return claim;
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

async function _twilioPhoneDetails(phone) {
  return await _callLambdaFunction("twilio-integration", {
    action: "phoneNumberInfo",
    phone,
  });
}

async function _callLambdaFunction(lambdaName, payload, fullResponse) {
  var params = {
    FunctionName: lambdaName, // the lambda function we are going to invoke
    InvocationType: "RequestResponse",
    LogType: "Tail",
    Payload: JSON.stringify(payload),
  };

  const command = new InvokeCommand(params);
  let response = await lambdaClient.send(command);
  response = new TextDecoder().decode(response.Payload);
  console.log(response);
  response = JSON.parse(response);
  if (fullResponse) {
    return response;
  }
  if (!response.success) {
    return undefined;
  } else {
    return response.content;
  }
}
