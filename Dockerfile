FROM python:3.9
WORKDIR /search
RUN apt-get update \
&& apt-get install -y curl source \
&& curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
&& curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
&& apt-get update \
&& ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
&& ACCEPT_EULA=Y apt-get install -y mssql-tools \
&& echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
&& source ~/.bashrc \
&& apt-get install -y unixODBC unixODBC-dev \
&& apt-get install -y libgssapi-krb5-2
COPY requirements.txt ./
RUN pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY . .
RUN mkdir /search/search/static
ENV LANG C.UTF-8
CMD ["gunicorn", "manage:app", "-c", "./config/gunicorn.conf.py"]