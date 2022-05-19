apt-get update
apt-get install git curl wget python make g++ -y
export NODE_OPTIONS=--openssl-legacy-provider
export NG_CLI_ANALOYTICS=false
wget -qO- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
nvm install 17
curl https://www.npmjs.com/install.sh | sh
npm install -g npm
npm install sass
git clone https://github.com/OpenTSDB/opentsdb-horizon.git
cd opentsdb-horizon/frontend
npm install --legacy-peer-deps
npm run build
cd ../..
cp -r opentsdb-horizon/server/public src/resources/docker/