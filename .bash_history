cd sbtree1
git reset --soft HEAD~1
git config --global user.name "Wang"
git config --global user.email "pjson233@gmail.com"
pwd
git rev-parse --show-toplevel
git remote add origin https://github.com/pjson233-lang/Tree-epoch.git
git remote -v
git pull origin main --allow-unrelated-histories
git branch
git status
pwd
cd /home/www/sbtree1
code .
git add .
git commit -m "上传所有项目文件"
rm -rf .git
git init
git add .
git commit -m "Initial commit for TSDB-tree"
git remote add origin https://github.com/pjson233-lang/TSDB-tree.git
git branch -M main
git push -u origin main
