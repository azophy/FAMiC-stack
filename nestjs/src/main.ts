import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const logger = new Logger('RequestLogger');
  
  // Middleware to log all requests
  app.use((req, res, next) => {
    const logData = {
      timestamp: new Date().toISOString(),
      method: req.method,
      url: req.url,
      ip: req.ip
    };
    logger.log(JSON.stringify(logData));
    next();
  });
  
  await app.listen(3000);
}
bootstrap();
