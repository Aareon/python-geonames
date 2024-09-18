import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, mock_open
from geonames.data_processing import load_data_in_chunks, process_chunk
from geonames.utils import get_column_info

@pytest.fixture
def sample_data():
    return (
        "US\t90210\tBeverly Hills\tCalifornia\tCA\t\t\t\t\t34.0901\t-118.4065\t4\n"
        "CA\tH3Z\tMontreal\tQuebec\tQC\t\t\t\t\t45.4850\t-73.5800\t3\n"
    )

@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        'country_code': ['US', 'CA'],
        'postal_code': ['90210', 'H3Z'],
        'place_name': ['Beverly Hills', 'Montreal'],
        'admin_name1': ['California', 'Quebec'],
        'admin_code1': ['CA', 'QC'],
        'admin_name2': ['', ''],
        'admin_code2': ['', ''],
        'admin_name3': ['', ''],
        'admin_code3': ['', ''],
        'latitude': [34.0901, 45.4850],
        'longitude': [-118.4065, -73.5800],
        'accuracy': [4, 3]
    })

def test_load_data_in_chunks(sample_data):
    with patch('builtins.open', mock_open(read_data=sample_data)), \
         patch('pathlib.Path.exists', return_value=True), \
         patch('pandas.read_csv') as mock_read_csv:
        mock_df = pd.DataFrame({'postal_code': ['90210', 'H3Z']})
        mock_read_csv.return_value = [mock_df]
        chunks = list(load_data_in_chunks(Path('test.txt'), chunksize=2))
        assert len(chunks) == 1
        assert isinstance(chunks[0], pd.DataFrame)
        assert chunks[0]['postal_code'].tolist() == ['90210', 'H3Z']

def test_load_data_in_chunks_file_not_found():
    with patch('pathlib.Path.exists', return_value=False):
        with pytest.raises(FileNotFoundError):
            list(load_data_in_chunks(Path('nonexistent.txt')))

def test_load_data_in_chunks_empty_file(tmp_path):
    empty_file = tmp_path / "empty.txt"
    empty_file.write_text("")
    chunks = list(load_data_in_chunks(empty_file))
    assert len(chunks) == 1
    assert chunks[0].empty

def test_process_chunk(sample_dataframe):
    result = process_chunk(sample_dataframe)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]['postal_code'] == '90210'
    assert result[1]['postal_code'] == 'H3Z'

def test_process_chunk_empty():
    empty_df = pd.DataFrame(columns=get_column_info().keys())
    result = process_chunk(empty_df)
    assert isinstance(result, list)
    assert len(result) == 0

def test_process_chunk_missing_columns(sample_dataframe):
    df_missing_columns = sample_dataframe.drop(columns=['country_code', 'postal_code'])
    result = process_chunk(df_missing_columns)
    assert isinstance(result, list)
    assert len(result) == 0

def test_process_chunk_data_types(sample_dataframe):
    result = process_chunk(sample_dataframe)
    assert isinstance(result[0]['latitude'], float)
    assert isinstance(result[0]['longitude'], float)
    assert isinstance(result[0]['accuracy'], int)
    assert isinstance(result[0]['country_code'], str)

def test_load_data_in_chunks_with_real_file(tmp_path):
    test_file = tmp_path / "test_data.txt"
    test_file.write_text(
        "US\t90210\tBeverly Hills\tCalifornia\tCA\t\t\t\t\t34.0901\t-118.4065\t4\n"
        "CA\tH3Z\tMontreal\tQuebec\tQC\t\t\t\t\t45.4850\t-73.5800\t3\n"
    )
    chunks = list(load_data_in_chunks(test_file, chunksize=1))
    assert len(chunks) == 2
    assert chunks[0]['postal_code'].values[0] == '90210'
    assert chunks[1]['postal_code'].values[0] == 'H3Z'