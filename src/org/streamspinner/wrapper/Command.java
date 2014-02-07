package org.streamspinner.wrapper;


public class Command {
	// �X�^�[�g��Ԃ�PC��MAC�v���R�}���h
	public static final byte[] REQUIRE_MAC = {(byte) 0x02, (byte) 0x01, (byte) 0x5b, (byte) 0x00, (byte) 0x00};
	// �L�^�Ԋu�i�������j�̐ݒ���擾
	public static final byte[] SAMP = {(byte) 0x02, (byte) 0x20, (byte) 0x01, (byte) 0x00, (byte) 0x00};
	// �L�^�Ԋu�i�������j��10ms�ɐݒ�
	public static final byte[] SAMP_10ms = {(byte) 0x02, (byte) 0x20, (byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0x00};
	// �L�^�Ԋu�i�������j��50ms�ɐݒ�
	public static final byte[] SAMP_50ms = {(byte) 0x02, (byte) 0x20, (byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0x02};
	// �L�^�Ԋu�i�������j��100ms�ɐݒ�
	public static final byte[] SAMP_100ms = {(byte) 0x02, (byte) 0x20, (byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0x03};
	// �X�^�[�g�R�}���h(���6�o�C�g��PC��MAC�A�h���X)
	public static final byte[] START = {(byte) 0x02, (byte) 0x01, (byte) 0x50, (byte) 0x00, (byte) 0x06
			, (byte) 0x00, (byte) 0x1a, (byte) 0x4d, (byte) 0x5b, (byte) 0x15, (byte) 0x3c};
	// ���������擪�ԍ��A�f�[�^���v���R�}���h1st�i�����j
	public static final byte[] REQUIRE_MEMORY = {(byte) 0x02, (byte) 0x01, (byte) 0x53, (byte) 0x00, (byte) 0x00};
	// �����ԗv���R�}���h
	public static final byte[] REQUIRE_STATE = {(byte) 0x02, (byte) 0x01, (byte) 0x57, (byte) 0x00, (byte) 0x00};
	// �A�v���V�X�e���g���K�R�}���h
	public static final byte[] SYSTRIGGER = {(byte) 0x02, (byte) 0x01, (byte) 0x58, (byte) 0x00, (byte) 0x09
			, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00
			, (byte) 0x00};
	// �f�[�^�v���R�}���h1st�i�����j
	public static final byte[] REQUIRE_DATA = {(byte) 0x02, (byte) 0x01, (byte) 0x55, (byte) 0x00, (byte) 0x10
			, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00
			, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01};
	// �X�g�b�v�R�}���h
	public static final byte[] STOP = {(byte) 0x02, (byte) 0x01, (byte) 0x51, (byte) 0x00, (byte) 0x00};
}
