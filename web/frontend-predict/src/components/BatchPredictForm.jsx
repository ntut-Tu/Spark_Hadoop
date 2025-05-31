import React, { useState } from "react";

export default function BatchPredictForm() {
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("");
  const [result, setResult] = useState(null);
  const API_BASE = "http://localhost:8001";

  const handleUpload = async (e) => {
    e.preventDefault();
    if (!file) return;

    const formData = new FormData();
    formData.append("file", file);

    const res = await fetch(`${API_BASE}/predict/batch/`, {
      method: "POST",
      body: formData,
    });
    const json = await res.json();
    const batchId = json.batch_id;

    setStatus("已上傳，正在處理中...");

    const interval = setInterval(async () => {
      const res = await fetch(`${API_BASE}/predict/batch/${batchId}`);
      if (res.ok) {
        const data = await res.json();
        if (data.status === "success") {
          clearInterval(interval);
          setResult(data.result);
          setStatus("批次預測完成");
        }
      }
    }, 5000);
  };

  return (
    <div>
      <h2>📂 批次預測</h2>
      <form onSubmit={handleUpload}>
        <input type="file" accept=".csv" onChange={(e) => setFile(e.target.files[0])} />
        <button type="submit">上傳並執行</button>
      </form>
      <p>{status}</p>
      {result && (
        <div>
          <h4>前 3 筆結果</h4>
          <pre>{JSON.stringify(result.slice(0, 3), null, 2)}</pre>
        </div>
      )}
    </div>
  );
}
