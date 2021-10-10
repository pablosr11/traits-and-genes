import express from "express";
import fs from "fs";
import cors from "cors";
import axios from "axios";

const app = express();
const port = 8080; // default port to listen
const HOST = `http://localhost:${port}`;

// specify what origins can send request
// null works for local html file, change when proxy server etc are setup
const corsOptions = {
  origin: `null`,
};

app.use(cors(corsOptions));

// start the Express server
app.listen(port, () => {
  console.log(`server started at ${HOST}`);
});

app.get("/", (req, res) => {
  //load html file directly instead
  // ideally this wouldnt exist as this endpoint is not allowd? Or make it welcome message to know backend is good.
  res.send("Wrong site");
});

app.get("/result/:jobId", (req, res) => {
  // when rest of api has the storage decoupled, we should pull the file from the file storage directly.
  // this endpoint might not even exist as frontend can diwnload from s3
  res.download(`../reports/report_${req.params["jobId"]}.csv`, () => {
    fs.unlink(`../reports/report_${req.params["jobId"]}.csv`, (err) => { console.error(err) });
  });

});

app.get("/status/:jobId", (req, res) => {
  // check if results file in  exists
  // this couples the state to the API. Ideally we check against the matching service status api
  // when processing is ready, download directly from file storage (s3 or whatever)
  fs.access(`../reports/report_${req.params["jobId"]}.csv`, fs.constants.F_OK, (err) => {
    if (err) {
      res.status(202).send("Not ready yet");
    } else {
      res.status(200).send(`${HOST}/result/${req.params["jobId"]}`);
    }
  })
});

app.post("/upload", async (req, res) => {
  let fileId = req.query["i"] as string;
  // use d query param to identify when upload is done. 
  var [chunkId, total] = (req.query["d"] as string).split("o");

  req.on("data", (chunk) => {
    // use chunk id to detect missing packets
    // this ties data to the api. ideally we could send it to a DB with fileid, chunkid and totalchunk.
    // and consolidate it later.
    fs.appendFileSync("../uploads/" + fileId + ".csv", chunk);
    console.log(
      `rcv ${chunkId} out of ${total} - ${chunk.length} goes in ${fileId}`
    );
  });

  // check if final header included
  // if yes, return link to results download
  if (chunkId === total) {
    console.log(`Upload completed: ${fileId}`);
    // trigger background job with ID fileId.
    await axios.post(`http://localhost:8000/candy/${fileId}`);
    res.status(200).send(`${HOST}/status/${fileId}`);
  } else {
    res.status(202).send("OK");
  }
});
