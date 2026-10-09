import { capture } from "../capture";
import { expect, test } from "../fixtures";

test("creates a source from the dashboard", async ({ page }) => {
  const name = `ui-qa.${Date.now()}`;

  await page.goto("/dashboard");
  await page.getByText("New source").click();
  await expect(page).toHaveURL(/\/sources\/new/);
  await page.getByPlaceholder("YourApp.SourceName").fill(name);

  await capture(page, {
    name: "new-source-01-form",
    expectations: [
      "The subhead reads ~/logs/new.",
      "The source name field shows the typed name, and an Add source button sits below the form.",
    ],
  });

  await page.getByRole("button", { name: "Add source" }).click();
  await expect(page.getByText("Source created!")).toBeVisible();
  await expect(page.getByText(name).first()).toBeVisible();

  await capture(page, {
    name: "new-source-02-created",
    expectations: [
      "A success flash reading Source created! is shown.",
      "The new source name appears on the page.",
    ],
  });
});
