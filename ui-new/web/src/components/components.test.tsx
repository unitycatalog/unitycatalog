import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Table as TableIcon } from "lucide-react";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

import MetaGrid from "@/components/MetaGrid";
import PropertiesCard from "@/components/PropertiesCard";
import DescriptionCard from "@/components/DescriptionCard";
import { QueryState } from "@/components/QueryState";
import Logo from "@/components/Logo";
import CatalogCrumbs from "@/components/CatalogCrumbs";
import EntityHeader from "@/components/EntityHeader";
import ThemeSwitcher from "@/components/ThemeSwitcher";
import { ThemeProvider } from "@/lib/theme";

describe("MetaGrid", () => {
  it("renders label/value pairs and an em dash for missing values", () => {
    render(<MetaGrid items={[{ label: "Owner", value: "me" }, { label: "ID", value: undefined }]} />);
    expect(screen.getByText("Owner")).toBeInTheDocument();
    expect(screen.getByText("me")).toBeInTheDocument();
    expect(screen.getByText("—")).toBeInTheDocument();
  });
});

describe("PropertiesCard", () => {
  it("shows a placeholder when empty", () => {
    render(<PropertiesCard properties={{}} />);
    expect(screen.getByText("No properties.")).toBeInTheDocument();
  });
  it("renders property rows", () => {
    render(<PropertiesCard properties={{ a: "1", b: "2" }} />);
    expect(screen.getByText("a")).toBeInTheDocument();
    expect(screen.getByText("2")).toBeInTheDocument();
  });
});

describe("DescriptionCard", () => {
  it("renders the comment or a placeholder", () => {
    const { rerender } = render(<DescriptionCard comment="hello" />);
    expect(screen.getByText("hello")).toBeInTheDocument();
    rerender(<DescriptionCard comment={undefined} />);
    expect(screen.getByText("No description provided.")).toBeInTheDocument();
  });
});

describe("QueryState", () => {
  it("renders a skeleton while loading", () => {
    const { container } = render(
      <QueryState isLoading error={null}>
        <div>data</div>
      </QueryState>,
    );
    expect(screen.queryByText("data")).not.toBeInTheDocument();
    expect(container.querySelector('[data-slot="skeleton"]')).toBeTruthy();
  });
  it("renders the error message", () => {
    render(
      <QueryState isLoading={false} error={new Error("boom")}>
        <div>data</div>
      </QueryState>,
    );
    expect(screen.getByText("boom")).toBeInTheDocument();
  });
  it("renders children when ready", () => {
    render(
      <QueryState isLoading={false} error={null}>
        <div>data</div>
      </QueryState>,
    );
    expect(screen.getByText("data")).toBeInTheDocument();
  });
});

describe("Logo", () => {
  it("renders an accessible titled svg", () => {
    render(<Logo title="Unity Catalog" />);
    expect(screen.getByRole("img", { name: "Unity Catalog" })).toBeInTheDocument();
  });
});

describe("CatalogCrumbs", () => {
  it("links ancestors and marks the leaf as the current page", () => {
    render(<CatalogCrumbs catalog="main" schema="default" leaf="events" />);
    const catalogLink = screen.getByRole("link", { name: "main" });
    expect(catalogLink).toHaveAttribute("href", "/catalog/main");
    const schemaLink = screen.getByRole("link", { name: "default" });
    expect(schemaLink).toHaveAttribute("href", "/catalog/main/default");
    // The leaf is the current page (aria-current), not a navigable anchor.
    const leaf = screen.getByText("events");
    expect(leaf).toHaveAttribute("aria-current", "page");
    expect(leaf).not.toHaveAttribute("href");
  });
});

describe("EntityHeader", () => {
  it("renders name, badges and actions", () => {
    render(
      <EntityHeader
        name="events"
        Icon={TableIcon}
        catalog="main"
        schema="default"
        badges={["MANAGED", "DELTA"]}
        actions={<button>Edit</button>}
      />,
    );
    expect(screen.getByRole("heading", { name: "events" })).toBeInTheDocument();
    expect(screen.getByText("MANAGED")).toBeInTheDocument();
    expect(screen.getByText("DELTA")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Edit" })).toBeInTheDocument();
  });
});

describe("ThemeSwitcher", () => {
  it("switches the active color mode", async () => {
    render(
      <ThemeProvider>
        <ThemeSwitcher />
      </ThemeProvider>,
    );
    await userEvent.click(screen.getByRole("button", { name: /dark theme/i }));
    expect(document.documentElement.dataset.colorMode).toBe("dark");
    expect(screen.getByRole("button", { name: /dark theme/i })).toHaveAttribute("aria-pressed", "true");
  });
});
