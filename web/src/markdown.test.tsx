import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { AnswerMarkdown } from "./markdown";

const noop = () => undefined;

describe("AnswerMarkdown", () => {
  it("renders markdown structure (headings, lists, emphasis, inline code)", () => {
    const { container } = render(
      <AnswerMarkdown
        text={"## Retrieval\n\nThe **hybrid** path uses `cosine` scores.\n\n- one\n- two"}
        onCite={noop}
      />,
    );
    expect(container.querySelector("h2")?.textContent).toBe("Retrieval");
    expect(container.querySelector("strong")?.textContent).toBe("hybrid");
    expect(container.querySelector("code")?.textContent).toBe("cosine");
    expect(Array.from(container.querySelectorAll("li")).map((li) => li.textContent)).toEqual([
      "one",
      "two",
    ]);
  });

  it("turns [E#] tokens into citation chips that call onCite", () => {
    const onCite = vi.fn();
    render(<AnswerMarkdown text="The parser changed [E2] to fix drift [E11]." onCite={onCite} />);
    const chips = screen.getAllByRole("button");
    expect(chips.map((c) => c.textContent)).toEqual(["E2", "E11"]);
    expect(chips.every((c) => c.className === "cite")).toBe(true);
    fireEvent.click(chips[0]);
    expect(onCite).toHaveBeenCalledWith("E2");
    fireEvent.click(chips[1]);
    expect(onCite).toHaveBeenCalledWith("E11");
  });

  it("keeps citation chips working inside markdown formatting", () => {
    const onCite = vi.fn();
    const { container } = render(
      <AnswerMarkdown text={"- **bold claim** [E3]\n- plain claim [E4]"} onCite={onCite} />,
    );
    const chips = screen.getAllByRole("button");
    expect(chips.map((c) => c.textContent)).toEqual(["E3", "E4"]);
    // The first chip lives inside the first list item, after the <strong>.
    expect(container.querySelectorAll("li")[0]?.contains(chips[0])).toBe(true);
    fireEvent.click(chips[0]);
    expect(onCite).toHaveBeenCalledWith("E3");
  });

  it("does not turn [E#] inside code into chips", () => {
    const { container } = render(
      <AnswerMarkdown
        text={"Use `arr[E1]` here.\n\n```\nconst x = arr[E2];\n```"}
        onCite={noop}
      />,
    );
    expect(screen.queryByRole("button")).toBeNull();
    expect(container.querySelector("code")?.textContent).toBe("arr[E1]");
    expect(container.querySelector("pre code")?.textContent).toContain("arr[E2]");
  });

  it("strips raw HTML from the answer (script, event handlers, unknown tags)", () => {
    const { container } = render(
      <AnswerMarkdown
        text={'Before <script>window.x = 1</script><img src="x" onerror="window.x=2"> <iframe src="https://example.com"></iframe> after'}
        onCite={noop}
      />,
    );
    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("iframe")).toBeNull();
    expect(container.textContent).toContain("Before");
    expect(container.textContent).toContain("after");
  });

  it("unwraps non-http(s) links to plain text", () => {
    const { container } = render(
      <AnswerMarkdown text={"a [bad](javascript:alert(1)) link"} onCite={noop} />,
    );
    expect(container.querySelector("a")).toBeNull();
    expect(container.textContent).toContain("bad");
  });

  it("renders http(s) links with a safe target/rel", () => {
    const { container } = render(
      <AnswerMarkdown text={"see [the docs](https://example.com/docs)"} onCite={noop} />,
    );
    const a = container.querySelector("a");
    expect(a?.getAttribute("href")).toBe("https://example.com/docs");
    expect(a?.getAttribute("target")).toBe("_blank");
    expect(a?.getAttribute("rel")).toBe("noopener noreferrer");
  });

  it("renders GFM tables", () => {
    const { container } = render(
      <AnswerMarkdown
        text={"| metric | value |\n| --- | --- |\n| recall | 0.93 |"}
        onCite={noop}
      />,
    );
    expect(container.querySelector("table")).not.toBeNull();
    expect(container.querySelector("th")?.textContent).toBe("metric");
    expect(container.querySelector("td")?.textContent).toBe("recall");
  });

  it("tolerates incomplete markdown mid-stream (unclosed fence, dangling emphasis)", () => {
    const { container } = render(
      <AnswerMarkdown text={"Partial **answer [E1]\n\n```py\nx ="} onCite={noop} />,
    );
    // No crash; the accumulated text still renders and the chip survives.
    expect(container.textContent).toContain("Partial");
    expect(screen.getAllByRole("button").map((c) => c.textContent)).toEqual(["E1"]);
  });
});
