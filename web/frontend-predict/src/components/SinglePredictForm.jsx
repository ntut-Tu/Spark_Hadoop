import React, {useState} from "react";
import {
    TextField,
    Select,
    MenuItem,
    Button,
    FormControl,
    InputLabel,
    Box,
    Typography,
    CircularProgress,
} from "@mui/material";

export default function SinglePredictForm() {
    const API_BASE = "http://localhost:8001";
    const [mode, setMode] = useState("modeB"); // 無需指定型別

    const formFieldsB = [
        {key: "Student_ID", label: "學號", type: "text"},
        {key: "Gender", label: "性別", type: "select", options: ["Male", "Female"]},
        {key: "Extracurricular_Activities", label: "是否參加課外活動", type: "select", options: ["Yes", "No"]},
        {key: "Internet_Access_at_Home", label: "家中是否有網路", type: "select", options: ["Yes", "No"]},
        {key: "Family_Income_Level", label: "家庭收入等級", type: "select", options: ["High", "Medium", "Low"]},
        {
            key: "Parent_Education_Level",
            label: "家長教育程度",
            type: "select",
            options: ["None", "High School", "Bachelor's", "Master's", "PhD"],
        },
        {key: "Department", label: "科系", type: "select", options: ["Mathematics", "Business", "Engineering", "CS"]},
        {key: "Grade", label: "成績等級", type: "select", options: ["A", "B", "C", "D", "F"]},
        {key: "Study_Hours_per_Week", label: "每週讀書時數", type: "number"},
        {key: "Total_Score", label: "總分", type: "number"},
        {key: "Projects_Score", label: "專題分數", type: "number"},
        {key: "Quizzes_Avg", label: "小考平均", type: "number"},
    ];

    const formFieldsA = [
        {key: "Student_ID", label: "學號", type: "text"},
        {key: "Gender", label: "性別", type: "select", options: ["Male", "Female"]},
        {key: "Sleep_Hours_per_Night", label: "每晚睡眠時數", type: "number"},
        {key: "Attendance (%)", label: "出席率 (%)", type: "number"},
        {key: "Stress_Level (1-10)", label: "壓力程度 (1~10)", type: "number"},
        {key: "Extracurricular_Activities", label: "是否參加課外活動", type: "select", options: ["Yes", "No"]},
        {key: "Internet_Access_at_Home", label: "家中是否有網路", type: "select", options: ["Yes", "No"]},
        {key: "Grade", label: "成績等級", type: "select", options: ["A", "B", "C", "D", "F"]},
        {key: "Total_Score", label: "總分", type: "number"},
        {key: "Projects_Score", label: "專題分數", type: "number"},
        {key: "Quizzes_Avg", label: "小考平均", type: "number"},
    ];

    const currentFields = mode === "modeA" ? formFieldsA : formFieldsB;

    const buildInitialFormData = (fields) => {
        const init = {};
        fields.forEach((field) => {
            if (field.type === "select") {
                init[field.key] = Array.isArray(field.options) ? field.options[0] : "";
            } else if (field.type === "number") {
                init[field.key] = 0;
            } else {
                init[field.key] = "";
            }
        });
        return init;
    };

    const [formData, setFormData] = useState(() =>
        buildInitialFormData(mode === "modeA" ? formFieldsA : formFieldsB)
    );

    const [result, setResult] = useState(null);
    const [status, setStatus] = useState("");

    const handleModeChange = (e) => {
        const newMode = e.target.value;
        setMode(newMode);
        setFormData(buildInitialFormData(newMode === "modeA" ? formFieldsA : formFieldsB));
        setResult(null);
        setStatus("");
    };

    const handleChange = (e) => {
        const {name, value, type} = e.target;
        setFormData((prev) => ({
            ...prev,
            [name]: type === "number" ? parseFloat(value) : value,
        }));
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setStatus("送出預測中...");
        setResult(null);

        try {
            const response = await fetch(`${API_BASE}/predict/`, {
                method: "POST",
                headers: {"Content-Type": "application/json"},
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
                } else {
                    setStatus("預測中...");
                }
            }, 3000);
        } catch (err) {
            console.error("Fetch error:", err);
            setStatus("錯誤：無法送出預測");
        }
    };

    return (
        <Box sx={{maxWidth: 600, mx: "auto", p: 2}}>
            <Typography variant="h5" gutterBottom>
                單一學生預測
            </Typography>

            {/* 模式選擇 */}
            <FormControl fullWidth sx={{mb: 2}}>
                <InputLabel>選擇模式</InputLabel>
                <Select value={mode} label="選擇模式" onChange={handleModeChange}>
                    <MenuItem value="modeA">模式 A（心理相關）</MenuItem>
                    <MenuItem value="modeB">模式 B（學業背景）</MenuItem>
                </Select>
            </FormControl>

            <form onSubmit={handleSubmit}>
                <Box display="flex" flexDirection="column" gap={2}>
                    {currentFields.map((field) => (
                        <FormControl key={field.key} fullWidth>
                            {field.type === "select" ? (
                                <>
                                    <InputLabel>{field.label}</InputLabel>
                                    <Select
                                        name={field.key}
                                        value={formData[field.key]}
                                        label={field.label}
                                        onChange={handleChange}
                                    >
                                        {field.options.map((opt) => (
                                            <MenuItem key={opt} value={opt}>
                                                {opt}
                                            </MenuItem>
                                        ))}
                                    </Select>
                                </>
                            ) : (
                                <TextField
                                    name={field.key}
                                    type={field.type}
                                    label={field.label}
                                    value={formData[field.key]}
                                    onChange={handleChange}
                                    required
                                    inputProps={field.type === "number" ? {step: "0.1"} : {}}
                                />
                            )}
                        </FormControl>
                    ))}

                    <Button type="submit" variant="contained" color="primary">
                        送出預測
                    </Button>

                    <Typography variant="body1">
                        {status.includes("預測中") ? <CircularProgress size={20}/> : null} {status}
                    </Typography>
                </Box>
            </form>

            {/* 顯示預測結果 */}
            {result && (
                <Box mt={4}>
                    <Typography variant="h6">預測結果</Typography>
                    <Box sx={{backgroundColor: "#f5f5f5", p: 2, borderRadius: 2}}>
                        {mode === "modeB" && (
                            <>
                                <Typography>
                                    背景分群：<strong>{result.background_cluster_label}</strong>
                                </Typography>
                                <Typography>
                                    成績分群：<strong>{result.score_cluster_label}</strong>
                                </Typography>
                            </>
                        )}
                        {mode === "modeA" && (
                            <>
                                <Typography>
                                    成績分群：<strong>{result.score_cluster_label}</strong>
                                </Typography>
                                <Typography>
                                    心理分群：<strong>{result.mental_cluster_label}</strong>
                                </Typography>
                            </>
                        )}
                    </Box>
                </Box>
            )}
        </Box>
    );
}
