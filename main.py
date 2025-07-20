import argparse
import sys
import os

def main():
    parser = argparse.ArgumentParser(description="运行数据库故障测试模拟。")
    parser.add_argument('--test-mode', type=str, default='redo',
                        choices=['redo', 'undo'],
                        help="选择测试模式：'redo' (数据库崩溃恢复测试) 或 'undo' (事务回滚/撤销测试)。")
    
    args = parser.parse_args()

    if args.test_mode == 'redo':
        print("选择 Redo 测试模式。")
        try:
            from main_redo import run_redo_test
            run_redo_test()
        except ImportError:
            print("错误：无法导入 main_redo.py。请确保文件存在。")
            sys.exit(1)
    elif args.test_mode == 'undo':
        print("选择 Undo 测试模式。")
        try:
            from main_undo import run_undo_test
            run_undo_test()
        except ImportError:
            print("错误：无法导入 main_undo.py。请确保文件存在。")
            sys.exit(1)
    else:
        print(f"不支持的测试模式: {args.test_mode}")
        sys.exit(1)

if __name__ == '__main__':
    main()