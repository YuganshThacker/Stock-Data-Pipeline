import { Router, type IRouter, type Request, type Response } from "express";

const router: IRouter = Router();

const STOCK_API_PORT = 8001;

router.all("/stock-api/{*path}", async (req: Request, res: Response) => {
  try {
    const targetUrl = `http://127.0.0.1:${STOCK_API_PORT}${req.originalUrl}`;
    const headers: Record<string, string> = {};
    for (const [key, value] of Object.entries(req.headers)) {
      if (typeof value === "string" && key !== "host") {
        headers[key] = value;
      }
    }

    const fetchOptions: RequestInit = {
      method: req.method,
      headers,
    };

    if (req.method !== "GET" && req.method !== "HEAD" && req.body) {
      fetchOptions.body = JSON.stringify(req.body);
      headers["content-type"] = "application/json";
    }

    const response = await fetch(targetUrl, fetchOptions);
    const contentType = response.headers.get("content-type") || "";

    res.status(response.status);
    for (const [key, value] of response.headers.entries()) {
      if (key !== "transfer-encoding") {
        res.setHeader(key, value);
      }
    }

    if (contentType.includes("application/json")) {
      const data = await response.json();
      res.json(data);
    } else {
      const text = await response.text();
      res.send(text);
    }
  } catch (error) {
    res.status(502).json({
      error: "Stock API service unavailable",
      detail: "The stock scraper API is not running. Start it with: python3 -m stock_scraper.app.api.main",
    });
  }
});

export default router;
