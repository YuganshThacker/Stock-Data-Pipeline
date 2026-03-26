import { pgTable, serial, text, numeric, jsonb, timestamp, integer, uniqueIndex, index } from "drizzle-orm/pg-core";

export const companyData = pgTable("company_data", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  symbol: text("symbol"),
  sector: text("sector"),
  industry: text("industry"),
  screenerUrl: text("screener_url"),

  marketCap: numeric("market_cap"),
  currentPrice: numeric("current_price"),
  pe: numeric("pe"),
  stockPe: numeric("stock_pe"),
  industryPe: numeric("industry_pe"),
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
  intrinsicValue: numeric("intrinsic_value"),
  pledgedPct: numeric("pledged_pct"),
  promoterHolding: numeric("promoter_holding"),
  opm: numeric("opm"),

  about: text("about").default(""),
  pros: text("pros").array().default([]),
  cons: text("cons").array().default([]),

  profitLoss: jsonb("profit_loss").default({}),
  balanceSheet: jsonb("balance_sheet").default({}),
  cashFlow: jsonb("cash_flow").default({}),
  quarterlyResults: jsonb("quarterly_results").default({}),
  shareholding: jsonb("shareholding").default({}),
  ratios: jsonb("ratios").default({}),

  news: jsonb("news").default([]),

  ragContent: text("rag_content").default(""),

  dataQuality: text("data_quality").default("pending"),
  dataCompleteness: numeric("data_completeness").default("0"),
  scrapedAt: timestamp("scraped_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
}, (table) => [
  uniqueIndex("company_data_name_key").on(table.name),
  index("idx_cd_symbol").on(table.symbol),
  index("idx_cd_name").on(table.name),
  index("idx_cd_sector").on(table.sector),
  index("idx_cd_market_cap").on(table.marketCap),
  index("idx_cd_pe").on(table.pe),
  index("idx_cd_roe").on(table.roe),
  index("idx_cd_roce").on(table.roce),
  index("idx_cd_data_quality").on(table.dataQuality),
]);

export const scrapeLogs = pgTable("scrape_logs", {
  id: serial("id").primaryKey(),
  companyId: integer("company_id").notNull(),
  status: text("status").notNull(),
  errorMessage: text("error_message"),
  durationMs: integer("duration_ms"),
  scrapedAt: timestamp("scraped_at", { withTimezone: true }).defaultNow(),
}, (table) => [
  index("idx_scrape_logs_company_id").on(table.companyId),
  index("idx_scrape_logs_scraped_at").on(table.scrapedAt),
]);
