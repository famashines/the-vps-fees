import {
  CommandEnvelopeV1Schema,
  ReceivePaymentCommandPayloadV1Schema,
  RecordExpenseCommandPayloadV1Schema,
  ReverseChargeCommandPayloadV1Schema,
  ReverseExpenseCommandPayloadV1Schema,
  ReversePaymentCommandPayloadV1Schema,
  SetFeeScheduleCommandPayloadV1Schema,
} from "@famashines/the-vps-contracts";
import { Ajv2020 } from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

const ajv = new Ajv2020({ allErrors: true });
addFormats.default(ajv);
const compile = (schema: object) => ajv.compile(structuredClone(schema));

export const validEnvelope = compile(CommandEnvelopeV1Schema);
export const payloadValidators = {
  "fees.SetFeeSchedule": compile(SetFeeScheduleCommandPayloadV1Schema),
  "fees.ReceivePayment": compile(ReceivePaymentCommandPayloadV1Schema),
  "fees.ReverseCharge": compile(ReverseChargeCommandPayloadV1Schema),
  "fees.ReversePayment": compile(ReversePaymentCommandPayloadV1Schema),
  "fees.RecordExpense": compile(RecordExpenseCommandPayloadV1Schema),
  "fees.ReverseExpense": compile(ReverseExpenseCommandPayloadV1Schema),
};
export const errorsOf = (validate: { errors?: unknown }) => ajv.errorsText(validate.errors as never, { separator: "; " });
