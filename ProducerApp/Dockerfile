FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["ProducerApp/ProducerApp.csproj", "ProducerApp/"]
COPY ["MessageFlow.Kafka/MessageFlow.Kafka.csproj", "MessageFlow.Kafka/"]
COPY ["MessageFlow/MessageFlow.csproj", "MessageFlow/"]
RUN dotnet restore "ProducerApp/ProducerApp.csproj"
COPY . .
WORKDIR "/src/ProducerApp"
RUN dotnet build "./ProducerApp.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./ProducerApp.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ProducerApp.dll"]
