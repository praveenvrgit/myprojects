report_tuples = [
    ('/home/etl/ETL/graphs/image1.html', report1_date_object_or_string),
    ('/home/etl/ETL/graphs/image2.html', report2_date_object_or_string),
    ('/home/etl/ETL/graphs/image3.html', report3_date_object_or_string),
]
sorted(report_tuples, key=lambda reports: reports[1])   # sort by date
html = '<html><body>' #add anything else in here or even better 
                      #use a template that you read and complement
lastDate = None
for r in report_tuples:
    if not lastDate or not lastDate == r[1]:
        html += '<h3>%s</h3>' % (str(r[1]))
    html += '<a href="%s">Your Report Title</a>' % (r[0])

htmlFile = '/home/etl/ETL/graphs/final.html'
f = open(htmlFile, 'w')
f.write(html)
f.close()
