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

  // refactor, small testable functions. SINGLE RESPONSABILITY.
  reader.onload = async (event) => {
    const CHUNK_SIZE = 1_000_000; //1mb
    const chunkCount = Math.ceil(
      (<ArrayBuffer>event.target.result).byteLength / CHUNK_SIZE
    );
    const identifier = hashThis(fileName);
    const url = `http://localhost:8080/upload?i=${identifier}`;

    for (let chunkId = 0; chunkId < chunkCount + 1; chunkId++) {
      const filechunk = <ArrayBuffer>(
        event.target.result.slice(
          chunkId * CHUNK_SIZE,
          chunkId * CHUNK_SIZE + CHUNK_SIZE
        )
      );
      if (chunkId === chunkCount) {
        // last block
        await fetch(url + `&q=${theFile.size}`, {
          method: "POST",
          body: filechunk,
        })
          .then((response) => response.json())
          .then((data) => {
            resultsBtn.hidden = false;
            resultsBtn.textContent = "Results Here";
            resultsLink = data["link"];
          })
          .catch((error) => console.log(error));
      } else {
        await fetch(url, {
          method: "POST",
          body: filechunk,
        }).catch((error) => console.log(error));
      }
      // if a chunk fails, whole file becomes corrupt?

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
