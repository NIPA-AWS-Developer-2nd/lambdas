# Separate Storage 빌드
echo "📦 Separate Storage 빌드 중..."
cd separate-storage

# 기존 node_modules 삭제
rm -rf node_modules package-lock.json

# Lambda용 Sharp 설치
npm install --platform=linux --arch=x64 --production
npm rebuild sharp --platform=linux --arch=x64

# 빌드 확인
if [ ! -f "node_modules/sharp/build/Release/sharp-linux-x64.node" ]; then
    echo "⚠️  Sharp 바이너리가 제대로 빌드되지 않았습니다. Docker 방식을 사용하세요."
    cd ..
    exit 1
fi

zip -r ../separate-storage-optimizer-fixed.zip . -x "*.git*" "node_modules/.cache/*" "README.md" "iam-policy.json"
cd ..

echo "✅ Lambda용 빌드 완료!"
echo "📁 생성된 파일:"
echo "   - in-place-optimizer-fixed.zip"
echo "   - separate-storage-optimizer-fixed.zip"
echo ""
echo "🚀 이제 이 파일들을 AWS Lambda에 업로드하세요!"
