/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import * as jspb from 'google-protobuf'



export class JsoldValidationRequest extends jspb.Message {
  getJsonld(): string;
  setJsonld(value: string): JsoldValidationRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): JsoldValidationRequest.AsObject;
  static toObject(includeInstance: boolean, msg: JsoldValidationRequest): JsoldValidationRequest.AsObject;
  static serializeBinaryToWriter(message: JsoldValidationRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): JsoldValidationRequest;
  static deserializeBinaryFromReader(message: JsoldValidationRequest, reader: jspb.BinaryReader): JsoldValidationRequest;
}

export namespace JsoldValidationRequest {
  export type AsObject = {
    jsonld: string,
  }
}

export class ValidationReply extends jspb.Message {
  getValid(): boolean;
  setValid(value: boolean): ValidationReply;

  getMessage(): string;
  setMessage(value: string): ValidationReply;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ValidationReply.AsObject;
  static toObject(includeInstance: boolean, msg: ValidationReply): ValidationReply.AsObject;
  static serializeBinaryToWriter(message: ValidationReply, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ValidationReply;
  static deserializeBinaryFromReader(message: ValidationReply, reader: jspb.BinaryReader): ValidationReply;
}

export namespace ValidationReply {
  export type AsObject = {
    valid: boolean,
    message: string,
  }
}

