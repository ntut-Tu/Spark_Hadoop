import React, { useState } from "react";
import Papa from "papaparse";
import {
  Box,
  Container,
  Typography,
  Button,
  Paper,
  Input,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow
} from "@mui/material";

export default function BatchPredictForm() {
  const [file, setFile] = useState(null);
  const [csvData, setCsvData] = useState([]);
  const [status, setStatus] = useState("");
  const [result, setResult] = useState(null);
  const API_BASE = "http://localhost:8001";

  const handleFileChange = (e) => {
    const f = e.target.files[0];
    setFile(f);

    Papa.parse(f, {
      header: true,
      skipEmptyLines: true,
      complete: (results) => {
        setCsvData(results.data);
      },
    });
  };

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

  if (res.status === 200 && res.headers.get("Content-Type").includes("text/csv")) {
    clearInterval(interval);

    const csvText = await res.text();
    const parsed = Papa.parse(csvText, {
      header: true,
      skipEmptyLines: true,
    });

    setResult(parsed.data);
    setStatus("批次預測完成");
  } else if (res.status !== 202) {
    clearInterval(interval);
    setStatus("發生錯誤，請稍後再試");
  }
  }, 5000);
};

  return (
      <Container maxWidth="md" sx={{ mt: 4, mb: 4 }}>
        <Paper elevation={3} sx={{ p: 4 }}>
          <Typography variant="h5" gutterBottom> 批次預測</Typography>

          <Box component="form" onSubmit={handleUpload} sx={{ mb: 2, display: 'flex', gap: 2, alignItems: 'center' }}>
            <Input type="file" inputProps={{ accept: ".csv" }} onChange={handleFileChange} />
            <Button type="submit" variant="contained">上傳並執行</Button>
          </Box>

          <Typography variant="body1" color="text.secondary">{status}</Typography>

          {csvData.length > 0 && (
              <Box sx={{ mt: 4 }}>
                <Typography variant="h6"> CSV 預覽（共 {csvData.length} 筆）</Typography>
                <TableContainer sx={{ maxHeight: 300, mt: 1, border: '1px solid #ccc' }}>
                  <Table stickyHeader size="small">
                    <TableHead>
                      <TableRow>
                        {Object.keys(csvData[0]).map((col) => (
                            <TableCell key={col}>{col}</TableCell>
                        ))}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {csvData.map((row, i) => (
                          <TableRow key={i}>
                            {Object.values(row).map((val, j) => (
                                <TableCell key={j}>{val}</TableCell>
                            ))}
                          </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
          )}

          {result && (
              <Box sx={{ mt: 4 }}>
                <Typography variant="h6"> 預測結果（共 {result.length} 筆）</Typography>
                <TableContainer sx={{ maxHeight: 300, mt: 1, border: '1px solid #ccc' }}>
                  <Table stickyHeader size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>#</TableCell>
                        <TableCell>學號</TableCell>
                        <TableCell>背景分群</TableCell>
                        <TableCell>心理分群</TableCell>
                        <TableCell>成績分群</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {result.map((row, index) => (
                          <TableRow key={index}>
                            <TableCell>{index + 1}</TableCell>
                            <TableCell>{row.Student_ID}</TableCell>
                            <TableCell>{row.background_cluster_label}</TableCell>
                            <TableCell>{row.mental_cluster_label}</TableCell>
                            <TableCell>{row.score_cluster_label}</TableCell>
                          </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
          )}
        </Paper>
      </Container>
  );
}
