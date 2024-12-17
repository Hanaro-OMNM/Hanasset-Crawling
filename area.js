const fs = require('fs');
const csv = require('csv-parser');
const mysql = require('mysql');

const csvFilePath = 'area.csv';

const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'root1234',
  database: 'omnm',
});

// 좌표 문자열을 POINT 형태로 변환
function createPointString(longitude, latitude) {
  return `POINT(${latitude} ${longitude})`;
}

// city_code, sigungu_code, emd_code 계산
function extractCodes(code) {
  const city_code = `${code.toString().slice(0, 2)}00000000`;
  const sigungu_code = `${code.toString().slice(0, 4)}00000`;
  const emd_code = code;

  return { city_code, sigungu_code, emd_code };
}

// // 메인 함수: CSV 파싱 및 데이터 삽입
// async function parseCSVAndInsert() {
//   try {
//     const insertQuery = `
//       INSERT INTO area_code (
//         code, address, city_code, emd_code, sigungu_code,
//         city_name, sigungu_name, emd_name, coordinate
//       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?, 4326))
//     `;

//     fs.createReadStream(csvFilePath)
//       .pipe(csv({ headers: false }))
//       .on('data', async (row) => {
//         try {
//           // CSV 행에서 필요한 데이터 추출
//           const [code, address, longitude, latitude] = [
//             parseInt(row[0]),
//             row[1],
//             parseFloat(row[2]),
//             parseFloat(row[3]),
//           ];

//           const { city_code, sigungu_code, emd_code } = extractCodes(code);

//           const [city_name, sigungu_name, emd_name] = address.split(' ');

//           const coordinate = createPointString(longitude, latitude);

//           // 데이터 삽입
//           connection.query(insertQuery, [
//             code,
//             address,
//             city_code,
//             emd_code,
//             sigungu_code,
//             city_name,
//             sigungu_name,
//             emd_name,
//             coordinate,
//           ]);

//           console.log(`Inserted: ${address}`);
//         } catch (err) {
//           console.error('Error inserting row:', err.message);
//         }
//       })
//       .on('end', async () => {
//         await connection.commit();
//         console.log('CSV file successfully processed and data inserted.');
//         await connection.end();
//       });
//   } catch (error) {
//     await connection.rollback();
//     console.error('Transaction failed:', error.message);
//     await connection.end();
//   }
// }

// 메인 함수: CSV 파싱 및 데이터 삽입
async function parseCSVAndInsert() {
  const insertQuery = `
    INSERT INTO area_code (code, address, city_code, emd_code, sigungu_code, city_name, sigungu_name, emd_name, coordinate)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?, 4326))
  `;

  fs.createReadStream(csvFilePath)
    .pipe(csv({ headers: false }))
    .on('data', (row) => {
      try {
        // CSV 행에서 필요한 데이터 추출
        const [code, address, longitude, latitude] = [
          parseInt(row[0]),
          row[1],
          parseFloat(row[2]),
          parseFloat(row[3]),
        ];

        const { city_code, sigungu_code, emd_code } = extractCodes(code);
        const [city_name, sigungu_name, emd_name] = address.split(' ');
        const coordinate = createPointString(longitude, latitude);

        // 데이터 삽입
        connection.query(
          insertQuery,
          [
            code,
            address,
            city_code,
            emd_code,
            sigungu_code,
            city_name,
            sigungu_name,
            emd_name,
            coordinate,
          ],
          (err, results) => {
            if (err) {
              console.error('Error inserting data:', err);
            } else {
              console.log(`Inserted: ${address}`);
            }
          }
        );
      } catch (err) {
        console.error('Error processing row:', err.message);
      }
    })
    .on('end', () => {
      console.log('CSV file successfully processed and data inserted.');
      connection.end();
    });
}

connection.connect((err) => {
  if (err) {
    console.error('Error connecting to MySQL database:', err);
    return;
  }
  console.log('Connected to MySQL database.');

  parseCSVAndInsert();
});
