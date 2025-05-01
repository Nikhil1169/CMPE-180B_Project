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
//product performance analysis
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
  
  
app.post("/quarterly-product-sales", async (req, res) => {
    try {

      const sqlFilePath = path.join(process.cwd(), "queries","quarterly_product_sales.sql");
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


//customer behavior analysis
app.post("/customer-exp", async (req, res) => {
  try {

    const sqlFilePath = path.join(process.cwd(), "queries","customer_experience.sql");
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

app.post("/top-customers", async (req, res) => {
  try {

    const sqlFilePath = path.join(process.cwd(), "queries","high_value_customer.sql");
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

app.post("/segments", async (req, res) => {
  try {

    const sqlFilePath = path.join(process.cwd(), "queries","segmenting_customers.sql");
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


//return rate analysis
app.post("/max-returns", async (req, res) => {
  try {

    const sqlFilePath = path.join(process.cwd(), "queries","top_returned_products.sql");
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


app.post("/loss-from-returns", async (req, res) => {
  try {

    const sqlFilePath = path.join(process.cwd(), "queries","loss_from_returns.sql");
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

app.post("/quarterly-return-rate", async (req, res) => {
  try {

    const sqlFilePath = path.join(process.cwd(), "queries","quarterly_return_rate.sql");
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

//marketing campaing analysis
app.post("/campaign", async (req, res) => {
  try {

    const sqlFilePath = path.join(process.cwd(), "queries","campaign_performance.sql");
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

app.post("/cac", async (req, res) => {
  try {

    const sqlFilePath = path.join(process.cwd(), "queries","cac.sql");
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

app.post("/conersion-rate", async (req, res) => {
  try {

    const sqlFilePath = path.join(process.cwd(), "queries","email_conversion_rate.sql");
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

