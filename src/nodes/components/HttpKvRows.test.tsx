import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { HttpKvRows } from "./HttpKvRows";
import type { HttpFetchKv } from "../../types/flow";

describe("HttpKvRows", () => {
  it("invokes onAdd when add is clicked", () => {
    const onAdd = vi.fn();
    render(
      <HttpKvRows
        sectionLabel="Query params"
        rows={[]}
        onAdd={onAdd}
        onUpdate={() => {}}
        onRemove={() => {}}
        emptyMessage="No rows."
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Add Query params row/i }));
    expect(onAdd).toHaveBeenCalledTimes(1);
  });

  it("invokes onRemove for a row", () => {
    const onRemove = vi.fn();
    const rows: HttpFetchKv[] = [{ id: "r1", key: "a", value: "b" }];
    render(
      <HttpKvRows
        sectionLabel="Headers"
        rows={rows}
        onAdd={() => {}}
        onUpdate={() => {}}
        onRemove={onRemove}
        emptyMessage="No rows."
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Remove Headers row/i }));
    expect(onRemove).toHaveBeenCalledWith("r1");
  });
});
