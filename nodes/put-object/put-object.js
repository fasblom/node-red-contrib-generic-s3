module.exports = function S3PutObject(RED) {
  const nodeInstance = instanceNode(RED);
  RED.nodes.registerType("Put Object", nodeInstance);
};

function instanceNode(RED) {
  return function nodeInstance(n) {
    RED.nodes.createNode(this, n); // Getting options for the current node
    this.conf = RED.nodes.getNode(n.conf); // Getting configuration
    let config = this.conf ? this.conf : null; // Cheking if the conf is valid
    if (!config) {
      this.warn(RED._("Missing S3 Client Configuration!"));
      return;
    }
    // Bucket parameter
    this.bucket = n.bucket !== "" ? n.bucket : null;
    // Object key parameter
    this.key = n.key !== "" ? n.key : null;
    // Body of the object to upload
    this.body = n.body !== "" ? n.body : null;
    // Upsert flag
    this.stream = n.stream ? n.stream : false;
    // Metadata of the object
    this.metadata = n.metadata !== "" ? n.metadata : null;
    // Content-Type of the object
    this.contentType = n.contentType !== "" ? n.contentType : null;
    // Upsert flag
    this.upsert = n.upsert ? n.upsert : false;
    // ACL permissions
    this.acl = n.acl && n.acl !== "" ? n.acl : false;
    // Content Encoding parameter
    this.contentencoding = n.contentencoding != "" ? n.contentencoding : null;
    // Input Handler
    this.on("input", inputHandler(this, RED));
  };
}

function inputHandler(n, RED) {
  return async function nodeInputHandler(msg, send, done) {
    // Imports
    const { S3 } = require("@aws-sdk/client-s3");
    const {
      isString,
      isJsonString,
      isObject,
      stringToStream,
      isValidContentEncoding,
      isValidACL,
    } = require("../../common/common");
    const crypto = require("crypto");

    let bucket = n.bucket ? n.bucket : msg.bucket;
    if (!bucket) {
      this.error("No bucket provided!");
      return;
    }

    let key = n.key ? n.key : msg.key;
    if (!key) {
      this.error("No object key provided!");
      return;
    }

    let body = n.body ? n.body : msg.body;
    if (!body) {
      this.error("No body data provided to put in the object!");
      return;
    }

    let stream = n.stream ? n.stream : msg.stream;

    if (!isString(body) && !stream) {
      this.error("The body should be formatted as string!");
      return;
    }

    let contentType = n.contentType ? n.contentType : msg.contentType;
    if (!contentType) {
      this.error("No Content-Type provided!");
      return;
    }

    let metadata = n.metadata ? n.metadata : msg.metadata;
    if (metadata && !isObject(metadata) && !isJsonString(metadata)) {
      this.error("The metadata should be of type Object!");
      return;
    }

    if (!isObject(metadata) && metadata) {
      metadata = JSON.parse(metadata);
    }

    let upsert = n.upsert ? n.upsert : msg.upsert;

    let contentencoding = n.contentencoding ? n.contentencoding : msg.contentencoding;
    if (contentencoding && !isValidContentEncoding(contentencoding)) {
      this.error("Invalid content encoding!");
      return;
    }

    let acl = n.acl ? n.acl : msg.acl;
    if (acl && !isValidACL(acl)) {
      this.error("Invalid ACL permissions value");
      return;
    }

    let s3Client = null;
    try {
      s3Client = new S3({
        endpoint: n.conf.endpoint,
        forcePathStyle: n.conf.forcepathstyle,
        region: n.conf.region,
        credentials: {
          accessKeyId: n.conf.credentials.accesskeyid,
          secretAccessKey: n.conf.credentials.secretaccesskey,
        },
      });

      this.status({ fill: "blue", shape: "dot", text: "Uploading" });

      let bodyToUpload = stream ? body : stringToStream(body);
      if (!bodyToUpload) {
        throw new Error("Failed to streamify body. Body needs to be a string!");
      }

      const objectToCreate = {
        Bucket: bucket,
        Key: key,
        ContentType: contentType,
        Body: bodyToUpload,
        ContentEncoding: contentencoding,
        ACL: acl,
        Metadata: metadata,
      };

      const result = await s3Client.putObject(objectToCreate);

      msg.payload = result; // Replace the payload with the result
      msg.key = key; // Append the object key to the message

      send(msg);
      this.status({ fill: "green", shape: "dot", text: "Success" });
    } catch (err) {
      this.status({ fill: "red", shape: "dot", text: "Failure" });
      msg.payload = null; // Clear payload on error
      msg.error = err;
      this.error(err, msg);
      send(msg);
    } finally {
      if (s3Client) s3Client.destroy();
      setTimeout(() => {
        this.status({});
      }, 3000);
      done();
    }
  };
}

