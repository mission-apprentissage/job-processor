import { describe, expect, it } from "vitest";
import { helloWorld } from "./index.ts";

describe("helloWorld", () => {
  it("should display hello world", () => {
    expect(helloWorld()).toBe("Hello World");
  });
});
