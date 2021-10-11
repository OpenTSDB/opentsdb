apt-get update
apt-get install curl wget python make g++ -y
wget -qO- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
nvm install 14
curl https://www.npmjs.com/install.sh | sh
npm install -g npm@6.14.8
npm install node-sass grunt-sass
git clone https://github.com/OpenTSDB/opentsdb-horizon.git
cd opentsdb-horizon/frontend
free
npm cache clean --force
npm install
export NODE_OPTIONS=--max_old_space_size=4064
npm run build
cd ../..
cp -r opentsdb-horizon/server/public src/resources/docker/