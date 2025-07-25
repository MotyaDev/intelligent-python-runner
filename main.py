import subprocess
import sys
import time
import logging
import signal
import os
import threading
import json
import psutil
import statistics
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Union
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import pickle

@dataclass
class ProcessMetrics:
    """Metrics for process performance"""
    start_time: float
    end_time: Optional[float] = None
    exit_code: Optional[int] = None
    cpu_usage: List[float] = None
    memory_usage: List[float] = None
    restart_reason: str = ""
    error_count: int = 0
    runtime_duration: float = 0.0

class IntelligentBotRunner:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize intelligent bot runner"""
        default_config = {
            'bot_files': ['enze.py'],
            'max_restarts_per_hour': 5,
            'min_runtime_threshold': 30,
            'health_check_interval': 15,
            'performance_window': 100,
            'adaptive_restart_delay': True,
            'intelligent_error_detection': True,
            'resource_monitoring': True,
            'predictive_restart': True,
            'learning_enabled': True,
            'auto_optimize': True,
            'log_file': 'intelligent_runner.log',
            'metrics_file': 'bot_metrics.pkl',
            'base_restart_delay': 10,
            'max_restart_delay': 600,
            'critical_memory_threshold': 500,
            'critical_cpu_threshold': 90,
            'error_pattern_threshold': 3,
            'smart_log_filtering': True,  # Умная фильтрация логов
        }
        
        self.config = {**default_config, **(config or {})}
        self.current_processes = {}
        self.shutdown_requested = False
        self.file_loggers = {}
        self.process_metrics = defaultdict(list)
        self.error_patterns = defaultdict(deque)
        self.performance_history = defaultdict(list)
        self.learned_patterns = {}
        self.restart_strategies = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # Паттерны для определения реальных ошибок
        self.error_patterns_regex = [
            r'ERROR|Error|error|CRITICAL|Critical|critical',
            r'Exception|exception|Traceback|traceback',
            r'Failed|failed|FAILED',
            r'Cannot|cannot|CANNOT',
            r'Unable|unable|UNABLE',
            r'Invalid|invalid|INVALID',
            r'Timeout|timeout|TIMEOUT'
        ]
        
        # Паттерны для НЕ-ошибок (которые могут быть в stderr но являются нормальными)
        self.non_error_patterns = [
            r'INFO|info|Debug|debug|DEBUG',
            r'Successfully|successfully|SUCCESS',
            r'Started|started|Starting|starting',
            r'Initialized|initialized|Connected|connected',
            r'Polling|polling|Running|running'
        ]
        
        self.setup_logging()
        self.load_historical_data()
        
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.logger.info("🧠 Intelligent Bot Runner initialized")
        self.logger.info(f"Managing {len(self.config['bot_files'])} bot files")

    def setup_logging(self):
        """Advanced logging setup"""
        try:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(self.config['log_file'], encoding='utf-8'),
                    logging.StreamHandler()
                ]
            )
            self.logger = logging.getLogger("IntelligentRunner")
            self.logger.info("✅ Logging system initialized")
        except Exception as e:
            print(f"Failed to setup logging: {e}")
            self.logger = logging.getLogger("IntelligentRunner")

    def is_real_error(self, message: str) -> bool:
        """Умное определение настоящих ошибок"""
        if not self.config['smart_log_filtering']:
            return True  # Если умная фильтрация отключена, считаем все ошибками
        
        message_lower = message.lower()
        
        # Сначала проверяем НЕ-ошибки
        for pattern in self.non_error_patterns:
            if re.search(pattern, message, re.IGNORECASE):
                return False
        
        # Затем проверяем настоящие ошибки
        for pattern in self.error_patterns_regex:
            if re.search(pattern, message, re.IGNORECASE):
                return True
        
        # Специальные проверки
        # Если в сообщении есть уровень логирования, используем его
        if re.search(r'- (INFO|DEBUG) -', message):
            return False
        if re.search(r'- (ERROR|CRITICAL|WARNING) -', message):
            return True
        
        # По умолчанию НЕ считаем ошибкой
        return False

    def classify_exit_code(self, exit_code: int) -> Dict[str, Any]:
        """Классификация кода завершения"""
        classifications = {
            0: {'type': 'success', 'description': 'Normal exit', 'restart': False},
            1: {'type': 'error', 'description': 'General error', 'restart': True},
            2: {'type': 'error', 'description': 'Misuse of shell command', 'restart': True},
            126: {'type': 'error', 'description': 'Command cannot execute', 'restart': True},
            127: {'type': 'error', 'description': 'Command not found', 'restart': False},
            128: {'type': 'signal', 'description': 'Invalid signal', 'restart': True},
            130: {'type': 'signal', 'description': 'Ctrl+C (SIGINT)', 'restart': False},
            137: {'type': 'signal', 'description': 'Process killed (SIGKILL)', 'restart': True},
            143: {'type': 'signal', 'description': 'Process terminated (SIGTERM)', 'restart': False},
            -9: {'type': 'signal', 'description': 'Killed by system (OOM/SIGKILL)', 'restart': True},
            -15: {'type': 'signal', 'description': 'Terminated (SIGTERM)', 'restart': False},
        }
        
        return classifications.get(exit_code, {
            'type': 'unknown', 
            'description': f'Unknown exit code: {exit_code}', 
            'restart': True
        })

    def load_historical_data(self):
        """Load historical performance and learning data"""
        try:
            metrics_file = Path(self.config['metrics_file'])
            if metrics_file.exists():
                try:
                    with open(metrics_file, 'rb') as f:
                        data = pickle.load(f)
                        self.learned_patterns = data.get('learned_patterns', {})
                        self.restart_strategies = data.get('restart_strategies', {})
                        self.performance_history = data.get('performance_history', defaultdict(list))
                    self.logger.info("📊 Historical data loaded successfully")
                except Exception as e:
                    self.logger.error(f"Failed to load historical data: {e}")
                    self.learned_patterns = {}
                    self.restart_strategies = {}
                    self.performance_history = defaultdict(list)
            else:
                self.logger.info("📊 No historical data found, starting fresh")
                self.learned_patterns = {}
                self.restart_strategies = {}
                self.performance_history = defaultdict(list)
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"Error in load_historical_data: {e}")
            else:
                print(f"Error in load_historical_data: {e}")

    def save_historical_data(self):
        """Save performance data and learned patterns"""
        try:
            data = {
                'learned_patterns': self.learned_patterns,
                'restart_strategies': self.restart_strategies,
                'performance_history': dict(self.performance_history),
                'last_updated': datetime.now().isoformat()
            }
            with open(self.config['metrics_file'], 'wb') as f:
                pickle.dump(data, f)
            self.logger.info("💾 Historical data saved")
        except Exception as e:
            self.logger.error(f"Failed to save historical data: {e}")

    def create_file_logger(self, file_path: Path) -> logging.Logger:
        """Create intelligent logger for file"""
        file_name = file_path.stem
        logger_name = f"Bot_{file_name}"
        
        if logger_name in self.file_loggers:
            return self.file_loggers[logger_name]
        
        try:
            file_logger = logging.getLogger(logger_name)
            file_logger.setLevel(logging.INFO)
            
            formatter = logging.Formatter(f'%(asctime)s - [{file_name}] %(message)s')
            
            file_handler = logging.FileHandler(f'{file_name}_detailed.log', encoding='utf-8')
            file_handler.setFormatter(formatter)
            
            error_handler = logging.FileHandler(f'{file_name}_errors.log', encoding='utf-8')
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            
            file_logger.addHandler(file_handler)
            file_logger.addHandler(error_handler)
            file_logger.addHandler(console_handler)
            file_logger.propagate = False
            
            self.file_loggers[logger_name] = file_logger
            return file_logger
        except Exception as e:
            self.logger.error(f"Failed to create logger for {file_name}: {e}")
            return self.logger

    def signal_handler(self, signum, frame):
        """Intelligent shutdown with data preservation"""
        if hasattr(self, 'logger'):
            self.logger.info(f"🛑 Received signal {signum}. Intelligent shutdown initiated...")
        else:
            print(f"🛑 Received signal {signum}. Shutting down...")
        self.shutdown_requested = True
        self.save_historical_data()
        self.stop_all_processes()

    def start_file_process(self, file_path: Path) -> Optional[subprocess.Popen]:
        """Intelligent process startup with optimization"""
        if not file_path.exists():
            self.logger.error(f"❌ File {file_path} not found!")
            return None
        
        file_logger = self.create_file_logger(file_path)
        
        try:
            file_logger.info(f"🚀 Starting process...")
            
            process = subprocess.Popen(
                [sys.executable, str(file_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
                cwd=file_path.parent
            )
            
            file_logger.info(f"✅ Process started with PID: {process.pid}")
            return process
            
        except Exception as e:
            file_logger.error(f"❌ Failed to start process: {e}")
            return None

    def monitor_process_output(self, file_path: Path, process: subprocess.Popen):
        """ИСПРАВЛЕННЫЙ мониторинг с умной фильтрацией логов"""
        file_logger = self.create_file_logger(file_path)
        error_count = 0
        
        def read_stdout():
            while process.poll() is None and not self.shutdown_requested:
                try:
                    line = process.stdout.readline()
                    if line:
                        clean_line = line.strip()
                        # stdout всегда логируем как INFO
                        file_logger.info(f"📤 {clean_line}")
                except Exception as e:
                    file_logger.error(f"Error reading stdout: {e}")
                    break
                    
        def read_stderr():
            nonlocal error_count
            while process.poll() is None and not self.shutdown_requested:
                try:
                    line = process.stderr.readline()
                    if line:
                        clean_line = line.strip()
                        
                        # УМНАЯ ФИЛЬТРАЦИЯ: проверяем, является ли это настоящей ошибкой
                        if self.is_real_error(clean_line):
                            file_logger.error(f"🚨 REAL ERROR: {clean_line}")
                            error_count += 1
                        else:
                            # Это НЕ ошибка, просто информация в stderr
                            file_logger.info(f"📥 {clean_line}")
                            
                except Exception as e:
                    file_logger.error(f"Error reading stderr: {e}")
                    break
        
        # Start monitoring threads
        stdout_thread = threading.Thread(target=read_stdout, daemon=True)
        stderr_thread = threading.Thread(target=read_stderr, daemon=True)
        
        stdout_thread.start()
        stderr_thread.start()
        
        return error_count

    def run_single_file_intelligently(self, file_path: Path):
        """Intelligent single file execution with smart restart logic"""
        file_logger = self.create_file_logger(file_path)
        restart_count = 0
        
        while not self.shutdown_requested:
            start_time = time.time()
            
            process = self.start_file_process(file_path)
            if not process:
                file_logger.error("❌ Failed to start process")
                break
            
            self.current_processes[str(file_path)] = process
            
            # Monitor process with smart filtering
            error_count = self.monitor_process_output(file_path, process)
            
            # Wait for process completion
            return_code = process.wait()
            end_time = time.time()
            runtime_duration = end_time - start_time
            
            # Classify exit code
            exit_classification = self.classify_exit_code(return_code)
            
            file_logger.info(f"📊 Process ended: code={return_code}, type={exit_classification['type']}, runtime={runtime_duration:.1f}s")
            file_logger.info(f"📄 Exit description: {exit_classification['description']}")
            
            if return_code == 0:
                file_logger.info(f"✅ Process completed successfully")
                break
            elif self.shutdown_requested:
                break
            elif not exit_classification['restart']:
                file_logger.info(f"🔄 Exit code {return_code} indicates no restart needed")
                break
            else:
                restart_count += 1
                
                # Умная логика перезапуска
                if exit_classification['type'] == 'signal' and return_code in [-9, 137]:
                    file_logger.warning(f"⚡ Process was killed (probably OOM or system), restart #{restart_count}")
                else:
                    file_logger.warning(f"🔄 Process needs restart: {exit_classification['description']} (restart #{restart_count})")
                
                # Prevent infinite restarts
                if restart_count >= 10:
                    file_logger.error("🛑 Too many restarts, stopping execution")
                    break
                
                # Smart restart delay based on exit code
                if exit_classification['type'] == 'signal':
                    delay = self.config['base_restart_delay'] * 2  # Longer delay for signals
                else:
                    delay = min(self.config['base_restart_delay'] * restart_count, self.config['max_restart_delay'])
                
                file_logger.info(f"⏳ Smart restart in {delay} seconds...")
                
                # Interruptible delay
                for _ in range(delay):
                    if self.shutdown_requested:
                        return
                    time.sleep(1)

    def stop_file_process(self, file_path: Path):
        """Stop process for file"""
        process = self.current_processes.get(str(file_path))
        if process and process.poll() is None:
            file_logger = self.create_file_logger(file_path)
            file_logger.info("🛑 Stopping process...")
            
            try:
                process.terminate()
                try:
                    process.wait(timeout=10)
                    file_logger.info("✅ Process stopped gracefully")
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    file_logger.warning("⚡ Process killed forcefully")
            except Exception as e:
                file_logger.error(f"❌ Error stopping process: {e}")
            finally:
                if str(file_path) in self.current_processes:
                    del self.current_processes[str(file_path)]

    def stop_all_processes(self):
        """Stop all processes"""
        if hasattr(self, 'logger'):
            self.logger.info("🛑 Stopping all processes...")
        for file_path in list(self.current_processes.keys()):
            self.stop_file_process(Path(file_path))

    def run_intelligent_management(self):
        """Main intelligent management loop"""
        self.logger.info("🧠 Starting Intelligent Bot Management System")
        
        # Validate all files exist
        valid_files = []
        for file_path_str in self.config['bot_files']:
            file_path = Path(file_path_str)
            if file_path.exists():
                valid_files.append(file_path)
                self.logger.info(f"✅ Bot file validated: {file_path}")
            else:
                self.logger.error(f"❌ Bot file not found: {file_path}")
        
        if not valid_files:
            self.logger.error("❌ No valid bot files found!")
            return
        
        try:
            # Run all files in parallel
            futures = []
            for file_path in valid_files:
                future = self.executor.submit(self.run_single_file_intelligently, file_path)
                futures.append(future)
            
            # Wait for all to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"❌ Error in execution: {e}")
                    
        except Exception as e:
            self.logger.error(f"💥 Critical error: {e}")
        finally:
            self.save_historical_data()
            self.stop_all_processes()
            self.executor.shutdown(wait=True)


def main():
    """Main function for intelligent bot runner"""
    config = {
        'bot_files': [
            'main.py',
        ],
        'max_restarts_per_hour': 5,
        'base_restart_delay': 15,
        'max_restart_delay': 300,
        'health_check_interval': 10,
        'smart_log_filtering': True,  # Включить умную фильтрацию логов
    }
    
    try:
        runner = IntelligentBotRunner(config)
        runner.run_intelligent_management()
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested")
    except Exception as e:
        print(f"💥 Critical error: {e}")
    finally:
        print("🧠 Intelligent Bot Runner terminated")


if __name__ == "__main__":
    main()
