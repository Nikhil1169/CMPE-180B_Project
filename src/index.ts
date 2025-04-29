import express from "express"
import { BigQuery } from "@google-cloud/bigquery";
import path from "path"
import fs from "fs"

const bigquery = new BigQuery({
    projectId: process.env.PROJECT_ID,
    keyFilename: path.join(process.cwd(), "service-account.json")
})

const app = express();
app.use(express.json());

app.post("/", async (req,res) => {
    try {
        const {cursor,limit=100} = req.body || {};
        const query = `SELECT * 
                    FROM \`bigquery-public-data.thelook_ecommerce.order_items\`
                    ${cursor ? `WHERE id > @cursor` : ''}
                    ORDER BY id
                    LIMIT @limit`;
        
        const [rows] = await bigquery.query({
            query,
            params: {
                cursor: cursor || null,
                limit
            },
            types: {
                cursor: 'INT64',
                limit: 'INT64'

            },
            location: "US"
        })

        res.json({
            data: rows,
            nextCursor: rows.length ? rows[rows.length-1].id : null
        })

    } catch(e){
        console.log(e)
        res.status(500).json({
            message: "Error while querying"
        })
    }
})

app.post("/top-products", async (req, res) => {
    try {
      // Read the SQL from the file
      const sqlFilePath = path.join(process.cwd(),"queries", "top_products.sql");
      const query = fs.readFileSync(sqlFilePath, "utf8");
  
      const [rows] = await bigquery.query({
        query,
        location: "US"
      });
  
      res.json({
        data: rows
      });
  
    } catch (e) {
      console.error(e);
      res.status(500).json({
        message: "Error while querying top products"
      });
    }
});
  

app.post("/underpeforming-items", async (req, res) => {
    try {
      // Read the SQL from the file
      const sqlFilePath = path.join(process.cwd(), "queries","underpeforming_items.sql");
      const query = fs.readFileSync(sqlFilePath, "utf8");
  
      const [rows] = await bigquery.query({
        query,
        location: "US"
      });
  
      res.json({
        data: rows
      });
  
    } catch (e) {
      console.error(e);
      res.status(500).json({
        message: "Error while querying inventory status"
      });
    }
});
  
  
app.post("/monthly-product-sales", async (req, res) => {
    try {

      const sqlFilePath = path.join(process.cwd(), "queries","monthly_product_sales.sql");
      const query = fs.readFileSync(sqlFilePath, "utf8");
  
      const [rows] = await bigquery.query({
        query,
        location: "US"
      });
  
      res.json({ data: rows });
    } catch (e) {
      console.error(e);
      res.status(500).json({
        message: "Error while querying monthly product sales"
      });
    }
});

app.listen(3010);
