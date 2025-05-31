import React, { useState } from "react";

export default function SinglePredictForm() {
  const API_BASE = "http://localhost:8001";

  const formFields = [
    { key: "Student_ID", label: "學號", type: "text" },
    { key: "Gender", label: "性別", type: "select", options: ["Male", "Female"] },
    { key: "Extracurricular_Activities", label: "是否參加課外活動", type: "select", options: ["Yes", "No"] },
    { key: "Internet_Access_at_Home", label: "家中是否有網路", type: "select", options: ["Yes", "No"] },
    { key: "Family_Income_Level", label: "家庭收入等級", type: "select", options: ["High", "Medium", "Low"] },
    {
      key: "Parent_Education_Level",
      label: "家長教育程度",
      type: "select",
      options: ["None", "High School", "Bachelor's", "Master's", "PhD"],
    },
    { key: "Department", label: "科系", type: "select", options: ["Mathematics", "Business", "Engineering", "CS"] },
    { key: "Grade", label: "成績等級", type: "select", options: ["A", "B", "C", "D", "F"] },
    { key: "Study_Hours_per_Week", label: "每週讀書時數", type: "number" },
    { key: "Final_Score", label: "期末總分", type: "number" },
  ];

  const [formData, setFormData] = useState(() => {
    const initial = {};
    formFields.forEach((field) => {
      if (field.type === "select") {
        initial[field.key] = field.options[0];
      } else {
        initial[field.key] = field.type === "number" ? 0 : "";
      }
    });
    return initial;
  });

  const [result, setResult] = useState(null);
  const [status, setStatus] = useState("");

  const handleChange = (e) => {
    const { name, value, type } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: type === "number" ? parseFloat(value) : value,
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setStatus("送出預測中...");

    try {
      const response = await fetch(`${API_BASE}/predict/`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData),
      });

      const data = await response.json();
      const sid = data.student_id || formData.Student_ID;
      if (!sid) {
        setStatus("無法取得 student_id");
        return;
      }

      const interval = setInterval(async () => {
        const res = await fetch(`${API_BASE}/predict/${sid}`);
        const json = await res.json();
        if (json.status === "done") {
          clearInterval(interval);
          setResult(json.result);
          setStatus("預測完成");
        } else if(!sid){
          clearInterval(interval);
          setStatus("錯誤：無法取得預測結果");
        } else{
          setStatus("預測中...");
        }
      }, 3000);
    } catch (err) {
      console.error("Fetch error:", err);
      setStatus("錯誤：無法送出預測");
    }
  };

  return (
    <div>
      <h2>🎓 單一學生預測</h2>
      <form onSubmit={handleSubmit}>
        {formFields.map((field) => (
          <div key={field.key}>
            <label>{field.label}：</label>
            {field.type === "select" ? (
              <select name={field.key} value={formData[field.key]} onChange={handleChange}>
                {field.options.map((opt) => (
                  <option key={opt} value={opt}>
                    {opt}
                  </option>
                ))}
              </select>
            ) : (
              <input
                name={field.key}
                type={field.type}
                value={formData[field.key]}
                onChange={handleChange}
                step="0.1"
                required
              />
            )}
          </div>
        ))}
        <button type="submit">送出預測</button>
      </form>
      <p>{status}</p>
      {result && (
        <div>
          <h4>預測結果</h4>
          <pre>{JSON.stringify(result, null, 2)}</pre>
        </div>
      )}
    </div>
  );
}
