import './App.css';
import Tabs from '@mui/material/Tabs';
import Tab from '@mui/material/Tab';
import React from "react";
import BatchPredictForm from "./components/BatchPredictForm";
import SinglePredictForm from "./components/SinglePredictForm";
import Box from "@mui/material/Box";

function App() {
    const [value, setValue] = React.useState('single');

    const handleChange = (event, newValue) => {
        setValue(newValue);
    };

    return (
        <Box sx={{ p: 4 }}>
            <h1>預測系統</h1>

            <Tabs value={value} onChange={handleChange}>
                <Tab label="單筆預測" value="single" />
                <Tab label="批次預測" value="batch" />
            </Tabs>

            <Box sx={{ mt: 4 }}>
                {value === 'single' && <SinglePredictForm />}
                {value === 'batch' && <BatchPredictForm />}
            </Box>
        </Box>
    );
}

export default App;
