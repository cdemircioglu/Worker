set mytime=%time%
echo Current time is %mytime% > Upload.txt

copy C:\Users\c81021292\workspace\GetJob\src\*.java .


git add .
git commit -m "Code update"
git push -u origin master