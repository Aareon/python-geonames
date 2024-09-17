import pandas as pd
from pathlib import Path
from unittest.mock import patch, mock_open
from geonames.data_processing import load_data_in_chunks, process_chunk

def test_load_data_in_chunks():
    test_data = "US\t90210\tBeverly Hills\tCalifornia\tCA\t\t\t\t\t34.0901\t-118.4065\t4\n"
    with patch('builtins.open', mock_open(read_data=test_data)), \
         patch('pathlib.Path.exists', return_value=True), \
         patch('pandas.read_csv') as mock_read_csv:
        mock_df = pd.DataFrame({'postal_code': ['90210']})
        mock_read_csv.return_value = [mock_df]
        chunks = list(load_data_in_chunks(Path('test.txt'), chunksize=1))
        assert len(chunks) == 1
        assert isinstance(chunks[0], pd.DataFrame)
        assert chunks[0]['postal_code'].values[0] == '90210'

def test_process_chunk():
    test_data = pd.DataFrame({
        'country_code': ['US'],
        'postal_code': ['90210'],
        'place_name': ['Beverly Hills'],
        'admin_name1': ['California'],
        'admin_code1': ['CA'],
        'admin_name2': [''],
        'admin_code2': [''],
        'admin_name3': [''],
        'admin_code3': [''],
        'latitude': [34.0901],
        'longitude': [-118.4065],
        'accuracy': [4]
    })
    result = process_chunk(test_data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]['postal_code'] == '90210'