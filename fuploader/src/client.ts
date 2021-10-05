const btnUpload = <HTMLButtonElement>document.getElementById("btnUpload");
const progressTracker = <HTMLDivElement>document.getElementById("progress");
const fileInput = <HTMLInputElement>document.getElementById("f");
const resultsBtn = <HTMLButtonElement>document.getElementById("results");
let resultsLink: string;

function hashThis(s: string) {
  let hash = 5381;
  let i = s.length;
  while (i) hash = (hash * 33) ^ s.charCodeAt(--i);
  return hash >>> 0;
}

function is_valid(theFile: File) {
  return (
    theFile.type === "text/csv" &&
    theFile.size <= 30_000_000 &&
    theFile.size >= 15_000_000
  );
}

async function handleUpload(
  r: Response,
  btn: HTMLButtonElement
): Promise<string> {
  btn.textContent = "Click here for Results";
  btn.hidden = false;
  return r.text();
}

function wait(delay: number) {
  return new Promise((resolve) => setTimeout(resolve, delay));
}

async function fetchRetry(
  url: string,
  delay: number = 1000, //1s
  tries: number,
  fetchOptions: RequestInit
): Promise<Response> {
  async function onError(err: Error) {
    let triesLeft = tries - 1;
    if (!triesLeft) {
      throw err;
    }
    return wait(delay).then(() =>
      fetchRetry(url, delay, triesLeft, fetchOptions)
    );
  }
  return fetch(url, fetchOptions).catch(onError);
}

async function uploadFile(
  ev: ProgressEvent<FileReader>,
  file_id: number,
  tracker: HTMLDivElement
) {
  const chunkSize = 1_000_000; //1mb
  const chunkCount = Math.ceil(
    (<ArrayBuffer>ev.target.result).byteLength / chunkSize
  );
  const url = `http://localhost:8080/upload?i=${file_id}`;
  for (let cid = 0; cid < chunkCount + 1; cid++) {
    const request_init: RequestInit = {
      method: "POST",
      body: ev.target.result.slice(
        cid * chunkSize,
        cid * chunkSize + chunkSize
      ),
    };
    const url_with_chunkid = url + `&d=${cid}o${chunkCount}`;

    // retry in case filechunk fails
    let r = await fetchRetry(url_with_chunkid, 100, 3, request_init);

    tracker.textContent =
      "Upload progress: " + Math.round((cid / chunkCount) * 100) + "%";

    // last block
    if (cid === chunkCount) {
      // modifying global var... annoying
      resultsLink = await handleUpload(r, resultsBtn);
    }
  }
}

      progressTracker.textContent =
        "Upload progress: " + Math.round((chunkId / chunkCount) * 100) + "%";
    }
  };
  reader.readAsArrayBuffer(theFile);
  f.value = "";
});

async function downloadResults(url: string, tracker: HTMLDivElement) {
  console.log(`Requesting ${url}`);
  let r = await fetch(url);
  if (r.status === 200) {
    tracker.textContent =
      "Completed, Download should have started. Click if it hasnt.";
    let link = await r.text();
    // replace this by a <a href> with download attribute?
    window.open(link);
  } else {
    tracker.textContent = "Not yet, try again in 1 minute";
  }
}

resultsBtn.addEventListener("click", (_) =>
  downloadResults(resultsLink, progressTracker)
);
