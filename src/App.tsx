import React, { useState } from 'react';
import axios, { AxiosError } from 'axios';
import {
  Container,
  Typography,
  TextField,
  Button,
  Paper,
  CircularProgress,
  Alert,
  Snackbar,
  FormControlLabel,
  Radio,
  RadioGroup,
} from '@mui/material';

interface ClickHouseConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  jwtToken: string;
  secure: boolean;
}

function App() {
  const [config, setConfig] = useState<ClickHouseConfig>({
    host: 'localhost',
    port: 8123,
    database: 'default',
    user: 'default',
    jwtToken: '',
    secure: false,
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>('');
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [columns, setColumns] = useState<string[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [exportResult, setExportResult] = useState<{count?: number; data?: string} | null>(null);
  const [sourceType, setSourceType] = useState<'clickhouse' | 'flatfile'>('clickhouse');

  const handleConnect = async () => {
    setLoading(true);
    try {
      const response = await axios.post('http://localhost:8000/connect-clickhouse', config);
      setTables(response.data.tables);
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      setError(error.response?.data?.detail || 'Connection failed');
    } finally {
      setLoading(false);
    }
  };

  const handleLoadColumns = async () => {
    if (!selectedTable) return;
    setLoading(true);
    try {
      const response = await axios.post('http://localhost:8000/get-columns', {
        ...config,
        table: selectedTable,
      });
      setColumns(response.data.columns);
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      setError(error.response?.data?.detail || 'Failed to load columns');
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setConfig(prev => ({
      ...prev,
      [name]: name === 'port' ? parseInt(value) : value
    }));
  };

  const handleColumnToggle = (column: string) => {
    setSelectedColumns(prev =>
      prev.includes(column) ? prev.filter(c => c !== column) : [...prev, column]
    );
  };

  const handleExport = async () => {
    if (!selectedTable || selectedColumns.length === 0) return;
    setLoading(true);
    try {
      const response = await axios.post('http://localhost:8000/clickhouse-to-flatfile', {
        ...config,
        table: selectedTable,
        columns: selectedColumns,
      });
      setExportResult(response.data);
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      setError(error.response?.data?.detail || 'Export failed');
    } finally {
      setLoading(false);
    }
  };

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    if (!event.target.files) return;
    const file = event.target.files[0];
    setLoading(true);
    try {
      const formData = new FormData();
      formData.append('file', file);
      formData.append('table', 'imported_data');
      formData.append('delimiter', ',');
      formData.append('host', config.host);
      formData.append('port', config.port.toString());
      formData.append('database', config.database);
      formData.append('user', config.user);
      formData.append('jwtToken', config.jwtToken);
      formData.append('secure', config.secure.toString());

      const response = await axios.post('http://localhost:8000/flatfile-to-clickhouse', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });
      setExportResult(response.data);
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      setError(error.response?.data?.detail || 'Import failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Container maxWidth="md" sx={{ mt: 4 }}>
      <Paper elevation={3} sx={{ p: 4 }}>
        <Typography variant="h4" gutterBottom>
          Data Ingestion Tool
        </Typography>

        <RadioGroup
          row
          value={sourceType}
          onChange={(e) => setSourceType(e.target.value as 'clickhouse' | 'flatfile')}
          sx={{ mb: 3 }}
        >
          <FormControlLabel value="clickhouse" control={<Radio />} label="ClickHouse to File" />
          <FormControlLabel value="flatfile" control={<Radio />} label="File to ClickHouse" />
        </RadioGroup>

        {sourceType === 'flatfile' ? (
          <div>
            <Typography variant="h6" sx={{ mb: 2 }}>Upload CSV File:</Typography>
            <input
              type="file"
              accept=".csv"
              onChange={handleFileUpload}
              disabled={loading}
            />
            {loading && <CircularProgress sx={{ mt: 2 }} />}
          </div>
        ) : (
          <>
            <TextField
              label="Host"
              name="host"
              fullWidth
              value={config.host}
              onChange={handleChange}
              sx={{ mb: 2 }}
            />
            <TextField
              label="Port"
              name="port"
              type="number"
              fullWidth
              value={config.port}
              onChange={handleChange}
              sx={{ mb: 2 }}
            />
            <TextField
              label="Database"
              name="database"
              fullWidth
              value={config.database}
              onChange={handleChange}
              sx={{ mb: 2 }}
            />
            <TextField
              label="User"
              name="user"
              fullWidth
              value={config.user}
              onChange={handleChange}
              sx={{ mb: 2 }}
            />
            <TextField
              label="JWT Token"
              name="jwtToken"
              fullWidth
              type="password"
              value={config.jwtToken}
              onChange={handleChange}
              sx={{ mb: 2 }}
            />
            
            <Button
              variant="contained"
              onClick={handleConnect}
              disabled={loading}
              sx={{ mb: 2 }}
            >
              {loading ? <CircularProgress size={24} /> : 'Connect'}
            </Button>

            {tables.length > 0 && (
              <>
                <Typography variant="h6" sx={{ mt: 2 }}>Select Table:</Typography>
                <select 
                  value={selectedTable}
                  onChange={(e) => setSelectedTable(e.target.value)}
                  style={{ width: '100%', padding: '8px', marginBottom: '10px' }}
                >
                  <option value="">Select a table</option>
                  {tables.map((table) => (
                    <option key={table} value={table}>{table}</option>
                  ))}
                </select>
                
                <Button
                  variant="contained"
                  onClick={handleLoadColumns}
                  disabled={!selectedTable || loading}
                  sx={{ mb: 2 }}
                >
                  {loading ? <CircularProgress size={24} /> : 'Load Columns'}
                </Button>
              </>
            )}

            {columns.length > 0 && (
              <>
                <Typography variant="h6" sx={{ mt: 2 }}>Select Columns:</Typography>
                <div style={{ maxHeight: '200px', overflowY: 'auto', marginBottom: '20px' }}>
                  {columns.map((column) => (
                    <div key={column}>
                      <input
                        type="checkbox"
                        id={column}
                        checked={selectedColumns.includes(column)}
                        onChange={() => handleColumnToggle(column)}
                      />
                      <label htmlFor={column} style={{ marginLeft: '8px' }}>{column}</label>
                    </div>
                  ))}
                </div>

                <Button
                  variant="contained"
                  color="primary"
                  onClick={handleExport}
                  disabled={selectedColumns.length === 0 || loading}
                >
                  {loading ? <CircularProgress size={24} /> : 'Export to CSV'}
                </Button>
              </>
            )}
          </>
        )}

        {exportResult && (
          <div style={{ marginTop: '20px' }}>
            <Typography>Exported {exportResult.count} records</Typography>
            <Button
              variant="outlined"
              onClick={() => {
                const blob = new Blob([exportResult.data!], { type: 'text/csv' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'export.csv';
                a.click();
              }}
              sx={{ mt: 1 }}
            >
              Download CSV
            </Button>
          </div>
        )}
      </Paper>

      <Snackbar open={!!error} autoHideDuration={6000} onClose={() => setError('')}>
        <Alert onClose={() => setError('')} severity="error" sx={{ width: '100%' }}>
          {error}
        </Alert>
      </Snackbar>
    </Container>
  );
}

export default App;