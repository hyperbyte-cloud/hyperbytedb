import assert from "node:assert/strict";
import { test } from "node:test";
import { encode, encodePoint } from "../dist/index.js";

test("escapes like the server", () => {
  const line = encodePoint({
    measurement: "cpu load,x",
    tags: { "host name": "a,b=c", empty: "" },
    fields: { "usage idle": 1.5, 'msg"': 'say "hi" \\ bye' },
    time: 123,
  });
  assert.equal(line, String.raw`cpu\ load\,x,host\ name=a\,b\=c msg"="say \"hi\" \\ bye",usage\ idle=1.5 123`);
});

test("field types: number is a float, bigint an integer", () => {
  assert.equal(encodePoint({ measurement: "m", fields: { f: 1, g: 0.5, i: 2n, b: true, s: "x", big: 1e21 } }), 'm b=true,big=1e+21,f=1,g=0.5,i=2i,s="x"');
});

test("non-finite, null and undefined fields are dropped", () => {
  assert.equal(encodePoint({ measurement: "m", fields: { a: NaN, b: Infinity, c: null, d: undefined, e: 1.5 } }), "m e=1.5");
  assert.equal(encodePoint({ measurement: "m", fields: { a: NaN } }), undefined);
  assert.equal(encode([{ measurement: "m", fields: { a: NaN } }, { measurement: "m", fields: { v: 1.5 } }]), "m v=1.5");
});

test("Date times follow the precision", () => {
  const t = new Date(Date.UTC(2026, 9, 12, 12, 0, 0, 500));
  const p = { measurement: "m", fields: { v: 1.5 }, time: t };
  assert.equal(encodePoint(p, "ns"), "m v=1.5 1791806400500000000");
  assert.equal(encodePoint(p, "u"), "m v=1.5 1791806400500000");
  assert.equal(encodePoint(p, "ms"), "m v=1.5 1791806400500");
  assert.equal(encodePoint(p, "s"), "m v=1.5 1791806400");
  assert.equal(encodePoint({ ...p, time: 1791806400500000001n }), "m v=1.5 1791806400500000001", "a bigint keeps every digit");
});

test("newlines, bad precisions and fractional times are refused", () => {
  assert.throws(() => encodePoint({ measurement: "m", tags: { host: "a\nb" }, fields: { v: 1 } }), TypeError);
  assert.throws(() => encodePoint({ measurement: "m", fields: { msg: "two\nlines" } }), TypeError);
  assert.throws(() => encodePoint({ measurement: "m", fields: { v: 1 } }, "minutes"), TypeError);
  assert.throws(() => encodePoint({ measurement: "m", fields: { v: 1 }, time: 1.5 }), TypeError);
});
