envName = "hf-dev"

# create new conda environment with the required packages
echo "creating new conda environment..."
conda create -n $envName python=3.6

# activate new environment
conda activate $envName

echo "installing packages..."
conda install -c conda-forge postgis pandas geopandas xarray dask

pip install rastersmith

mkdir hydrabuild
cd hydrabuild
git clone https://github.com/kmarkert/Landsat578.git
cd Landsat578
python setup.py install
cd ..
git clone https://github.com/servir-mekong/hydra-floods.git
cd hydra-floods
python setup.py install
cd ..

echo "Building database..."
psql -d postgres -f ./hydradb.sql

echo "Environment build is complete"
echo "use '$ conda activate hf-dev' to start environment"
