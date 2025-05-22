FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["ConsumerApp/ConsumerApp.csproj", "ConsumerApp/"]
COPY ["MessageFlow.Kafka/MessageFlow.Kafka.csproj", "MessageFlow.Kafka/"]
COPY ["MessageFlow/MessageFlow.csproj", "MessageFlow/"]
RUN dotnet restore "ConsumerApp/ConsumerApp.csproj"
COPY . .
WORKDIR "/src/ConsumerApp"
RUN dotnet build "./ConsumerApp.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./ConsumerApp.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ConsumerApp.dll"]
