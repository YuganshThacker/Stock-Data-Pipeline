import { pgTable, serial, text, numeric, jsonb, timestamp, integer, uniqueIndex, index } from "drizzle-orm/pg-core";

export const companies = pgTable("companies", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  symbol: text("symbol"),
  sector: text("sector"),
  industry: text("industry"),
  screenerUrl: text("screener_url"),
  marketCap: text("market_cap"),
  currentPrice: text("current_price"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
}, (table) => [
  uniqueIndex("companies_name_key").on(table.name),
  index("idx_companies_symbol").on(table.symbol),
  index("idx_companies_name").on(table.name),
]);

export const fundamentals = pgTable("fundamentals", {
  id: serial("id").primaryKey(),
  companyId: integer("company_id").notNull().references(() => companies.id, { onDelete: "cascade" }),
  marketCap: numeric("market_cap"),
  currentPrice: numeric("current_price"),
  pe: numeric("pe"),
  pb: numeric("pb"),
  roce: numeric("roce"),
  roe: numeric("roe"),
  debtToEquity: numeric("debt_to_equity"),
  eps: numeric("eps"),
  dividendYield: numeric("dividend_yield"),
  salesGrowth: numeric("sales_growth"),
  profitGrowth: numeric("profit_growth"),
  faceValue: numeric("face_value"),
  bookValue: numeric("book_value"),
  highLow: text("high_low"),
  stockPe: numeric("stock_pe"),
  industryPe: numeric("industry_pe"),
  intrinsicValue: numeric("intrinsic_value"),
  pledgedPct: numeric("pledged_pct"),
  changeInPromoterHolding: numeric("change_in_promoter_holding"),
  rawData: jsonb("raw_data").default({}),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
}, (table) => [
  uniqueIndex("fundamentals_company_id_key").on(table.companyId),
  index("idx_fundamentals_company_id").on(table.companyId),
]);

export const financials = pgTable("financials", {
  id: serial("id").primaryKey(),
  companyId: integer("company_id").notNull().references(() => companies.id, { onDelete: "cascade" }),
  profitLoss: jsonb("profit_loss").default({}),
  balanceSheet: jsonb("balance_sheet").default({}),
  cashFlow: jsonb("cash_flow").default({}),
  quarterly: jsonb("quarterly").default({}),
  shareholding: jsonb("shareholding").default({}),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
}, (table) => [
  uniqueIndex("financials_company_id_key").on(table.companyId),
  index("idx_financials_company_id").on(table.companyId),
]);

export const ratios = pgTable("ratios", {
  id: serial("id").primaryKey(),
  companyId: integer("company_id").notNull().references(() => companies.id, { onDelete: "cascade" }),
  ratiosData: jsonb("ratios_data").default({}),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
}, (table) => [
  uniqueIndex("ratios_company_id_key").on(table.companyId),
  index("idx_ratios_company_id").on(table.companyId),
]);

export const insights = pgTable("insights", {
  id: serial("id").primaryKey(),
  companyId: integer("company_id").notNull().references(() => companies.id, { onDelete: "cascade" }),
  pros: text("pros").array().default([]),
  cons: text("cons").array().default([]),
  about: text("about").default(""),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
}, (table) => [
  uniqueIndex("insights_company_id_key").on(table.companyId),
  index("idx_insights_company_id").on(table.companyId),
]);

export const news = pgTable("news", {
  id: serial("id").primaryKey(),
  companyId: integer("company_id").notNull().references(() => companies.id, { onDelete: "cascade" }),
  title: text("title").notNull(),
  newsDate: text("news_date"),
  source: text("source"),
  link: text("link"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
}, (table) => [
  uniqueIndex("news_company_id_title_key").on(table.companyId, table.title),
  index("idx_news_company_id").on(table.companyId),
]);

export const scrapeLogs = pgTable("scrape_logs", {
  id: serial("id").primaryKey(),
  companyId: integer("company_id").notNull().references(() => companies.id, { onDelete: "cascade" }),
  status: text("status").notNull(),
  errorMessage: text("error_message"),
  durationMs: integer("duration_ms"),
  scrapedAt: timestamp("scraped_at", { withTimezone: true }).defaultNow(),
}, (table) => [
  index("idx_scrape_logs_company_id").on(table.companyId),
  index("idx_scrape_logs_scraped_at").on(table.scrapedAt),
]);
